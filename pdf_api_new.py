from flask import Flask
from flask import request
from flask import jsonify
import json
import pdfquery
import requests
import bs4
import os
import mysql.connector
import datetime

# # local testing
db_host = "localhost"
db_user = "root"
db_password = ""
db_name = "benefit_db"
server_host = "localhost"
server_port = 5005

# server testing
# db_host = "209.126.98.140"
# db_user = "pedro"
# db_password = "pedro123"
# db_name = "benefit_db"
# server_host = "209.126.98.140"
# server_port = 5005

app = Flask(__name__)

scraping_running = False

@app.route("/extrato", methods=['GET'])
def extrato():
    content = request.args
    if not 'nb' in content:
        return "<h2>Missing parameters.</h2>"
    nb_str = content['nb']

    base_url = "http://ws.margemcerta.net.br/hiscre.aspx?emp=275&login=cc-hiscon158&senha=1PTpqOtu&nb={}"
    url = base_url.format(nb_str)
    r = requests.get(url)
    if not r.text.startswith("%PDF"):
        return r.text
        # "Horário não permitido"
        # 'Número de Benfício inválido.'
    pdf_filename = "{}.pdf".format(nb_str)
    if os.path.isfile(pdf_filename):
        os.remove(pdf_filename)
    pdf_file = open(pdf_filename, "wb")
    pdf_file.write(r.content)
    pdf_file.close()

    global scraping_running
    if scraping_running == True:
        return "<h2>Another instance of scraping is already running.</h2>"
    scraping_running = True
    result_str = do_scraping(pdf_filename)
    scraping_running = False
    return result_str


def do_scraping(filename):
    
    try:
        # open mysql connection
        mydb = mysql.connector.connect(
        host=db_host,
        user=db_user,
        passwd=db_password,
        database=db_name
        )
        mycursor = mydb.cursor()

        PDF_FILE = filename
        pdf = pdfquery.PDFQuery(PDF_FILE)
        pdf.load()
        data_tree = pdf.tree

        # data_tree.write("test2.xml", pretty_print=True, encoding="utf-8")

        pages = data_tree.xpath('//*/LTPage')
        cur_title = ""
        page_num = 0
        data = {}
        data['Contratos de Empréstimos']=[]
        data['Contratos de Cartão']=[]
        data['qr_code'] = ""
        for page in pages:
            page_num += 1
            all_lines = page.xpath('* // LTTextBoxHorizontal')
            for line in all_lines:
                if line.text.strip() == "":
                    all_lines.remove(line)
            sorted_lines = sorted(all_lines, key=lambda x: (int(float(x.attrib['y1'])+0.5),-int(float(x.attrib['x0']))+0.5), reverse=True)
            line_count = len(sorted_lines)
            line_index = 0
            while line_index < line_count:
                cur_str = sorted_lines[line_index].text.strip()
                print (cur_str)

                if "Demonstrado apenas empréstimos ativos e suspensos" in cur_str:
                    break

                if page_num > 1 and float(sorted_lines[line_index].attrib['y1']) > 477.0:
                    line_index += 1
                    continue

                if "INSS poderá rever a qualquer tempo as informações constantes deste extrato" in cur_str:
                    line_index += 1
                    continue

                if cur_str == "Dados do Beneficiário" or cur_str == "Dados do Benefício" or cur_str == "Margem Consignável" or cur_str == "Instituição Pagadora" or cur_str == "Contratos de Empréstimos" or cur_str == "Contratos de Cartão":
                    cur_title = cur_str
                    line_index += 1
                    continue

                
                if cur_title == "Dados do Beneficiário":
                    if "Nome:" in cur_str:
                        res_nome = cur_str.split(":")[1].strip()
                        if res_nome == "":
                            line_index +=1
                            cur_str = sorted_lines[line_index].text.strip()
                            if not "Número do Benefício:" in cur_str:
                                res_nome = cur_str.strip()
                        data['Nome'] = res_nome
                    elif "Número do Benefício:" in cur_str:
                        res_numero_beneficio = cur_str.split(":")[1].strip()
                        if res_numero_beneficio == "":
                            line_index +=1
                            cur_str = sorted_lines[line_index].text.strip()
                            if not "CPF:" in cur_str:
                                res_numero_beneficio = cur_str.strip()
                        data['Número do Benefício'] = res_numero_beneficio
                    elif "CPF:" in cur_str:
                        res_cpf = cur_str.split(":")[1].strip()
                        if res_cpf == "":
                            cur_str = sorted_lines[line_index+1].text.strip()
                            if not "Dados do Benefício" in cur_str:
                                res_cpf = cur_str.strip()
                                line_index += 1
                        data['CPF'] = res_cpf
                elif cur_title == "Dados do Benefício":
                    if "Espécie:" in cur_str:
                        res_especie = cur_str.split(":")[1].strip()
                        if res_especie == "":
                            line_index +=1
                            cur_str = sorted_lines[line_index].text.strip()
                            if not "Situação:" in cur_str:
                                res_especie = cur_str.strip()
                        data['Espécie'] = res_especie
                    elif "Situação:" in cur_str:
                        res_beneficio_situacao = cur_str.split(":")[1].strip()
                        if res_beneficio_situacao == "":
                            line_index +=1
                            cur_str = sorted_lines[line_index].text.strip()
                            if not "Características:" in cur_str:
                                res_beneficio_situacao = cur_str.strip()
                        data['Benefício Situação'] = res_beneficio_situacao
                    elif "É Pensão Alimentícia:" in cur_str:
                        res_alimenticia = cur_str.split(":")[1].strip()
                        if res_alimenticia == "":
                            line_index +=1
                            cur_str = sorted_lines[line_index].text.strip()
                            if not "Possui Representante Legal:" in cur_str:
                                res_alimenticia = cur_str.strip()
                        data['É Pensão Alimentícia'] = res_alimenticia
                    elif "Possui Representante Legal:" in cur_str:
                        res_representante_legal = cur_str.split(":")[1].strip()
                        if res_representante_legal == "":
                            line_index +=1
                            cur_str = sorted_lines[line_index].text.strip()
                            if not "Bloqueado para Empréstimo:" in cur_str:
                                res_representante_legal = cur_str.strip()
                        data['Possui Representante Legal'] = res_representante_legal
                    elif "Bloqueado para Empréstimo:" in cur_str:
                        res_bloqueado_emprestimo = cur_str.split(":")[1].strip()
                        if res_bloqueado_emprestimo == "":
                            line_index +=1
                            cur_str = sorted_lines[line_index].text.strip()
                            if not "Elegível para Empréstimo:" in cur_str:
                                res_bloqueado_emprestimo = cur_str.strip()
                        data['Bloqueado para Empréstimo'] = res_bloqueado_emprestimo
                    elif "Elegível para Empréstimo:" in cur_str:
                        res_elegivel_emprestimo = cur_str.split(":")[1].strip()
                        if res_elegivel_emprestimo == "":
                            cur_str = sorted_lines[line_index+1].text.strip()
                            if not "Margem Consignável" in cur_str:
                                res_elegivel_emprestimo = cur_str.strip()
                                line_index += 1
                        data['Elegível para Empréstimo'] = res_elegivel_emprestimo
                elif cur_title == "Margem Consignável":
                    if "Base de Cálculo:" in cur_str:
                        res_base_calculo = cur_str.split(":")[1].strip()
                        if res_base_calculo == "":
                            line_index +=1
                            cur_str = sorted_lines[line_index].text.strip()
                            if not "Margem para Empréstimo:" in cur_str:
                                res_base_calculo = cur_str.strip()
                        res_base_calculo = res_base_calculo.replace(".","").replace(",",".").replace("R$","").strip()
                        data['Base de Cálculo'] = res_base_calculo
                    elif "Margem para Empréstimo:" in cur_str:
                        res_margem_emprestimo = cur_str.split(":")[1].strip()
                        if res_margem_emprestimo == "":
                            line_index +=1
                            cur_str = sorted_lines[line_index].text.strip()
                            if not "Margem para Cartão:" in cur_str:
                                res_margem_emprestimo = cur_str.strip()
                        res_margem_emprestimo = res_margem_emprestimo.replace(".","").replace(",",".").replace("R$","").strip()
                        data['Margem para Empréstimo'] = res_margem_emprestimo
                    elif "Margem para Cartão:" in cur_str:
                        res_margem_cartao = cur_str.split(":")[1].strip()
                        if res_margem_cartao == "":
                            cur_str = sorted_lines[line_index+1].text.strip()
                            if not "Instituição Pagadora" in cur_str:
                                res_margem_cartao = cur_str.strip()
                                line_index += 1
                        res_margem_cartao = res_margem_cartao.replace(".","").replace(",",".").replace("R$","").strip()
                        data['Margem para Cartão'] = res_margem_cartao
                elif cur_title == "Instituição Pagadora":
                    if "CBC/Banco:" in cur_str:
                        res_cbc_banco = cur_str.split(":")[1].strip()
                        if res_cbc_banco == "":
                            line_index +=1
                            cur_str = sorted_lines[line_index].text.strip()
                            if not "Tipo:" in cur_str:
                                res_cbc_banco = cur_str.strip()
                        data['CBC/Banco'] = res_cbc_banco
                    elif "Tipo:" in cur_str:
                        res_tipo = cur_str.split(":")[1].strip()
                        if res_tipo == "":
                            line_index +=1
                            cur_str = sorted_lines[line_index].text.strip()
                            if not "Ag.:" in cur_str:
                                res_tipo = cur_str.strip()
                        data['Tipo'] = res_tipo
                    elif "Ag.:" in cur_str:
                        res_ag = cur_str.split(":")[1].strip()
                        if res_ag == "":
                            line_index +=1
                            cur_str = sorted_lines[line_index].text.strip()
                            if not "C/C.:" in cur_str:
                                res_ag = cur_str.strip()
                        data['Ag'] = res_ag
                    elif "C/C.:" in cur_str:
                        res_cc = cur_str.split(":")[1].strip()
                        if res_cc == "":
                            cur_str = sorted_lines[line_index+1].text.strip()
                            if not "Contratos de Empréstimos" in cur_str:
                                res_cc = cur_str.strip()
                                line_index += 1
                        data['C/C'] = res_cc
                elif cur_title == "Contratos de Empréstimos":
                    if cur_str in "Comp. Última QTD Empréstimo CBC / Banco Data inclusão Valor Parcela Valor Emprestado Parcela Parcelas":
                        line_index += 1
                        continue
                    first_item_y1 = float(sorted_lines[line_index].attrib['y1'])
                    first_item_y0 = float(sorted_lines[line_index].attrib['y0'])
                    tmp_lines = []
                    while True:
                        cur_item_y1 = float(sorted_lines[line_index].attrib['y1'])
                        cur_item_y0 = float(sorted_lines[line_index].attrib['y0'])
                        if (cur_item_y1>=first_item_y0 and cur_item_y1<=first_item_y1) or (cur_item_y0>=first_item_y0 and cur_item_y0<=first_item_y1):
                            tmp_lines.append(sorted_lines[line_index])
                            line_index += 1
                        else:
                            break
                    tmp_lines = sorted(tmp_lines, key=lambda x: float(x.attrib['x0']), reverse=False)
                    contracts_data = {}
                    res_emprestimo = ""
                    res_cbc_banco = ""
                    res_comp_parcela = ""
                    res_comp_ultima_parcela = ""
                    res_data_inclusao = ""
                    res_qtd_parcelas = ""
                    res_valor_parcela = ""
                    res_valor_parcela = ""
                    res_valor_emprestado = ""
                    try:
                        res_emprestimo = tmp_lines[0].text.strip()
                        res_cbc_banco = tmp_lines[1].text.strip()
                        res_comp_parcela = tmp_lines[2].text.strip()
                        res_comp_ultima_parcela = tmp_lines[3].text.strip()
                        res_data_inclusao = tmp_lines[4].text.strip()
                        res_qtd_parcelas = tmp_lines[5].text.strip()
                        res_valor_parcela = tmp_lines[6].text.strip()
                        res_valor_parcela = res_valor_parcela.replace(".","").replace(",",".").replace("R$","").strip()
                        res_valor_emprestado = tmp_lines[7].text.strip()
                        res_valor_emprestado = res_valor_emprestado.replace(".","").replace(",",".").replace("R$","").strip()
                    except:
                        pass
                    contracts_data['Empréstimo'] = res_emprestimo
                    contracts_data['CBC/Banco'] = res_cbc_banco
                    contracts_data['Comp Parcela'] = res_comp_parcela
                    contracts_data['Comp Última Parcela'] = res_comp_ultima_parcela
                    contracts_data['Data inclusão'] = res_data_inclusao
                    contracts_data['QTD Parcelas'] = res_qtd_parcelas
                    contracts_data['Valor Parcela'] = res_valor_parcela
                    contracts_data['Valor Emprestado'] = res_valor_emprestado
                    contracts_data['Situação'] = ""
                    cur_str = sorted_lines[line_index].text.strip()
                    if "Situação:" in cur_str:
                        res_situacao = cur_str.split(":")[1].strip()
                        if res_situacao == "":
                            cur_str = sorted_lines[line_index+1].text.strip()
                            if cur_str.strip() in "Ativo Inativo":
                                res_situacao = cur_str.strip()
                                line_index += 1
                        contracts_data['Situação'] = res_situacao
                        data['Contratos de Empréstimos'].append(contracts_data)
                        # modified code snippet
                        situation_line_pos_y = float(sorted_lines[line_index].attrib['y0'])
                        situation_line_height = float(sorted_lines[line_index].attrib['height'])
                        situation_offset_index = 1
                        next_line_pos_y = float(sorted_lines[line_index + situation_offset_index].attrib['y0'])
                        while abs(situation_line_pos_y - next_line_pos_y) < situation_line_height:
                            situation_offset_index += 1
                            next_line_pos_y = float(sorted_lines[line_index + situation_offset_index].attrib['y0'])
                        line_index = line_index + situation_offset_index - 1
                        # modified code snippet
                    else:
                        data['Contratos de Empréstimos'].append(contracts_data)
                        continue
                elif cur_title == "Contratos de Cartão":
                    if cur_str in "Nº Contrato CBC / Banco Data de Inclusão Situação Limite Valor":
                        line_index += 1
                        continue
                    res_num_contrado = cur_str
                    res_cbc_banco = sorted_lines[line_index+1].text.strip()
                    res_data_inclusao = sorted_lines[line_index+2].text.strip()
                    res_situacao = sorted_lines[line_index+3].text.strip()
                    res_limite = sorted_lines[line_index+4].text.strip()
                    res_limite = res_limite.replace(".","").replace(",",".").replace("R$","").strip()
                    res_valor = sorted_lines[line_index+5].text.strip()
                    res_valor = res_valor.replace(".","").replace(",",".").replace("R$","").strip()
                    line_index += 5
                    contracts_data = {}
                    contracts_data['No Contrato'] = res_num_contrado
                    contracts_data['CBC/Banco'] = res_cbc_banco
                    contracts_data['Data de Inclusão'] = res_data_inclusao
                    contracts_data['Situação'] = res_situacao
                    contracts_data['Limite'] = res_limite
                    contracts_data['Valor'] = res_valor
                    data['Contratos de Cartão'].append(contracts_data)
                line_index += 1

            all_lines = page.xpath('* // LTTextLineHorizontal')
            for line in all_lines:
                cur_str = line.text.strip()
                if "com o código" in cur_str:
                    data['qr_code'] = cur_str.replace("com o código", "").strip()
                    break

        # write to database : table name is `payroll_data`
        mycursor.execute("SELECT `Número do Benefício` FROM `payroll_data` WHERE `Número do Benefício`='{}'".format(data['Número do Benefício']))
        benefit_numbers = mycursor.fetchall()
        if len(benefit_numbers)>0:
            print ("That benefit number already exists!")
            return "That benefit number already exists!"
        else:
            sql = (
                "INSERT INTO `payroll_data` (`Nome`, `Número do Benefício`, `CPF`, `Espécie`, `Situação`, `É Pensão Alimentícia`, `Possui Representante Legal`, `Bloqueado para Empréstimo`, `Elegível para Empréstimo`, `Base de Cálculo`, `Margem para Empréstimo`, `Margem para Cartão`, `CBC/Banco`, `Tipo`, `Ag`, `C/C`, `QR código`)" \
                "VALUES" \
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            )

            val = [data['Nome'], data['Número do Benefício'], data['CPF'], data['Espécie'], data['Benefício Situação'], data['É Pensão Alimentícia'], data['Possui Representante Legal'], data['Bloqueado para Empréstimo'], data['Elegível para Empréstimo'], data['Base de Cálculo'], data['Margem para Empréstimo'], data['Margem para Cartão'], data['CBC/Banco'], data['Tipo'], data['Ag'], data['C/C'], data['qr_code']]
            mycursor.execute(sql, val)
            mydb.commit()

            # write to database : table name is `loan_contracts`
            sql = (
                "INSERT INTO `loan_contracts` (`Número do Benefício`, `Empréstimo`, `CBC/Banco`, `Comp Parcela`, `Comp Última Parcela`, `Data inclusão`, `QTD Parcelas`, `Valor Parcela`, `Valor Emprestado`, `Situação`)" \
                "VALUES" \
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            )
            tmps = data['Contratos de Empréstimos']
            for tmp in tmps:
                val = [data['Número do Benefício'], tmp['Empréstimo'], tmp['CBC/Banco'], tmp['Comp Parcela'], tmp['Comp Última Parcela'], tmp['Data inclusão'], tmp['QTD Parcelas'], tmp['Valor Parcela'], tmp['Valor Emprestado'], tmp['Situação']]
                mycursor.execute(sql, val)
                mydb.commit()


            # write to database : table name is `card_contracts`
            sql = (
                "INSERT INTO `card_contracts` (`Número do Benefício`, `No Contrato`, `CBC/Banco`, `Data de Inclusão`, `Situação`, `Limite`, `Valor`)" \
                "VALUES" \
                "(%s,%s,%s,%s,%s,%s,%s);"
            )
            tmps = data['Contratos de Cartão']
            for tmp in tmps:
                val = [data['Número do Benefício'], tmp['No Contrato'], tmp['CBC/Banco'], tmp['Data de Inclusão'], tmp['Situação'], tmp['Limite'], tmp['Valor']]
                mycursor.execute(sql, val)
                mydb.commit()

        print ("Finished Succesfully!")
        return "Scraping from PDF completed successfully."
    except Exception as ex:
        with open("log.txt", "a") as logfile:
            logfile.write("{}\n".format(filename))
        return ex.msg
        return "Error!"


if __name__ == '__main__':
    #app.run(debug=True)
    app.run(server_host, port=server_port)
    # result = do_scraping("12312312.pdf")
    # print (result)
