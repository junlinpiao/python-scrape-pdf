from flask import Flask
import PyPDF2
import collections
from flask import request
from flask import jsonify
import json
import re
from flask.json import jsonify
from multiprocessing.pool import ThreadPool
import traceback
import subprocess
import requests
import time
from bs4 import BeautifulSoup
import os, io


app = Flask(__name__)


def run(*popenargs, input=None, check=False, **kwargs):
    if input is not None:
        if 'stdin' in kwargs:
            raise ValueError('stdin and input arguments may not both be used.')
        kwargs['stdin'] = subprocess.PIPE


    process = subprocess.Popen(*popenargs,stdout=subprocess.PIPE,  **kwargs)
    try:
        stdout, stderr = process.communicate(input)
    except:
        process.kill()
        process.wait()
        raise
    retcode = process.poll()
    if check and retcode:
        raise subprocess.CalledProcessError(
            retcode, process.args, output=stdout, stderr=stderr)
    return retcode, stdout, stderr

def Getproxies():

    return  {'ip':'127.0.0.1','port':8888}
    #return json.loads(run( ['php', '/var/www/html/phpenterprise/proxy.php'])[1].decode('utf-8'))

@app.route("/consultamais", methods=['POST'])
def post_consultamais():
    content = request.get_json()
    if content is None:
        return jsonify({'success': False, 'message': 'missing parameters'})
    if 'cpf' not in content or 'user' not in content  or 'password' not in content:
        return jsonify({'success': False, 'message': 'missing parameters'})
    try:
        return consultaMais(content['cpf'], content['user'], content['password'], False)
    except Exception as ex:
        tb = traceback.format_exc()
        return jsonify({'success': False, 'message': 'unable to get data. Message error:' + str(ex) + tb})


@app.route("/consultamaisproxy", methods=['POST'])
def post_consultamaisproxy():
    content = request.get_json()
    if content is None:
        return jsonify({'success': False, 'message': 'missing parameters'})
    if 'cpf' not in content or 'user' not in content or 'password' not in content:
        return jsonify({'success': False, 'message': 'missing parameters'})
    try:
        return consultaMais(content['cpf'], content['user'], content['password'], True)
    except Exception as ex:
        tb = traceback.format_exc()
        return jsonify({'success': False, 'message': 'unable to get data. Message error:' + str(ex) + tb})



@app.route("/consultamaisproxy", methods=['GET'])
def get_consultamaisproxy():
    content = request.args
    if content is None:
        return jsonify({'success': False, 'message': 'missing parameters'})
    if 'cpf' not in content or 'user' not in content or 'password' not in content:
        return jsonify({'success': False, 'message': 'missing parameters'})
    try:
        print(content['cpf'])
        return consultaMais([content['cpf'],], content['user'], content['password'], True)
    except Exception as ex:
        tb = traceback.format_exc()
        return jsonify({'success': False, 'message': 'unable to get data. Message error:' + str(ex) + tb})

@app.route("/consultamais", methods=['GET'])
def get_consultamais():
    content = request.args
    if content is None:
        return jsonify({'success': False, 'message': 'missing parameters'})
    if 'cpf' not in content or 'user' not in content or 'password' not in content:
        return jsonify({'success': False, 'message': 'missing parameters'})
    try:

        return consultaMais([content['cpf'],], content['user'], content['password'], False)
    except Exception as ex:
        tb = traceback.format_exc()
        return jsonify({'success': False, 'message': 'unable to get data. Message error:' + str(ex) + tb})

def consultaMais(cpfs, user, password, useProxy):
    pool = ThreadPool(processes=1)
    resultList = []
    for cpf in cpfs:
        async_result = pool.apply_async(threadConsultaMais, ( str(cpf), user, password, useProxy))
        resultList.append(async_result)

    resultData = []
    for result in resultList:
        return_val = result.get()
        resultData.append(return_val)

    return jsonify({'success': True, 'data': resultData})


def threadConsultaMais( cpf, user, password, useProxy):
    print('hiudhiaus')
    sess = requests.session()
    if useProxy:
        proxiesJson = Getproxies()
        if proxiesJson:

            proxies = {
                'http': 'http://' + proxiesJson['ip'] + ':' + str(proxiesJson['port']),
                'https': 'https://' + proxiesJson['ip'] + ':' + str(proxiesJson['port'])
            }
            sess.proxies.update(proxies)

    # pdfResponse = sess.get('http://consultamais.servicos.ws/api/extrato/BuscarExtrato.php?user=' + user + '&password=' +
    #              password + '&cpf=' +cpf)

    #read_pdf('')#pdfResponse.content)
    stringa =convert_pdf_to_txt("C:\\1\\sample.pdf")
    beneficios = []
    oldBeneficio = ''
    beneficio = {}
    for x in stringa:

        regex = re.search('Número do Benefício:\s+(.*?)\\n', x['text'].decode('utf-8')).group(1)
        if regex != oldBeneficio:
            oldBeneficio = regex

            beneficio = {}
            beneficio['NB'] = regex
            beneficio['Nome'] = re.search('Nome:\s+(.*?)\\n', x['text'].decode('utf-8')).group(1)
            beneficio['CPF'] = re.search('CPF:\s+(.*?)\\n', x['text'].decode('utf-8')).group(1)
            beneficio['Especie'] = re.search('Espécie:\s+(.*?)\\n', x['text'].decode('utf-8')).group(1)
            beneficio['Situacao'] = re.search('Situação:\s+(.*?)\\n', x['text'].decode('utf-8')).group(1)
            beneficio['PensaoAlimenticia'] = re.search('É Pensão Alimentícia:\s+(.*?)\\n', x['text'].decode('utf-8')).group(1)
            beneficio['PossuiRepresentanteLegal'] = re.search('Possui Representante Legal:\s+(.*?)\\n', x['text'].decode('utf-8')).group(1)
            beneficio['BloqueadoParaEmprestimo'] = re.search('Bloqueado para Empréstimo:\s+(.*?)\\n', x['text'].decode('utf-8')).group(1)
            beneficio['ElegivelParaEmprestimo'] = re.search('Elegível para Empréstimo:\s+(.*?)\\n', x['text'].decode('utf-8')).group(1)
            beneficio['BaseCalculo'] = re.search('Base de Cálculo:\s+(.*?)\\n', x['text'].decode('utf-8')).group(1)
            beneficio['MargemParaEmprestimo'] = re.search('Margem para Empréstimo:\s+(.*?)\\n', x['text'].decode('utf-8')).group(1)
            beneficio['MargemParaCartao'] = re.search('Margem para Cartão:\s+(.*?)\\n', x['text'].decode('utf-8')).group(1)
            beneficio['CBCBanco'] = re.search('CBC/Banco:\s+(.*?)\\n', x['text'].decode('utf-8')).group(1)
            beneficio['Tipo'] = re.search('Tipo:\n\n(.*?)\n\nAg.:', x['text'].decode('utf-8')).group(1)
            beneficio['Agencia'] = re.search('Ag.:\n\n(.*?)\n\nC/C.:', x['text'].decode('utf-8')).group(1)
            beneficio['ContaCorrente'] = re.search('C/C.:\n\n(.*?)\n\nContratos de ', x['text'].decode('utf-8')).group(1)

            contratosEmprestimos = re.search('Valor Parcela Valor Emprestado(.*)?INSS',x['text'].decode('utf-8'),re.DOTALL).group(1)

            # print(str(int(contratosEmprestimos.count('\n\n'))))
            # print((contratosEmprestimos.split('\n\n')))
            beneficio['emprestimos'] = []
            beneficio['cartoes'] = []
            beneficios.append(beneficio)

        coluna1 = re.findall('\n(.*?)\n\nSituação:\n\n(.*?)\n',  x['text'].decode('utf-8'))
        rest = re.findall('\n(.*?)\n\n(\d{2}/\d{4})\n\n(\d{2}/\d{4})\n\n(\d{2}/\d{2}/\d{4})\n\n(\d+)\n\n(.*?)\n\n(.*?)\n',  x['text'].decode('utf-8'))
        print(x['text'].decode('utf-8'))
        for i in range(0, len(coluna1)):
            info = {}

            info['emprestimo'] = coluna1[i][0]
            info['situacao'] = coluna1[i][1]

            info['CBCBanco'] = rest[i][0]
            info['Comp1Parcela'] = rest[i][1]
            info['CompUltimaParcela'] = rest[i][2]
            info['DataInclusao'] = rest[i][3]
            info['QtdeParcelas'] = rest[i][4]
            info['ValorParcela'] = rest[i][5]
            info['ValorEmprestado'] = rest[i][6]
            beneficio['emprestimos'].append(info)



        coluna1 = re.findall('Nº Contrato\n\n(.*?)\n\n', x['text'].decode('utf-8'))
        rest = re.findall('CBC / Banco\n\n(.*?)\n\nData de Inclusão\n\nSituação\n\nLimite\n\nValor\n\n(\d{2}/\d{2}/\d{4})\n\n(.*?)\n\n(.*?)\n',
            x['text'].decode('utf-8'))

        for i in range(0, len(coluna1)):
            info = {}

            info['contrato'] = coluna1[i][0]
            info['CBCBanco'] = rest[i][0]

            info['DataInclusao'] = rest[i][1]
            info['Situação'] = rest[i][2]
            info['Limite'] = ''
            info['Valor'] = rest[i][3]

            beneficio['cartoes'].append(info)


    return {'success':True, 'data':beneficios}


from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from io import StringIO

def convert_pdf_to_txt(path):
    fp = open(path, 'rb')
    rsrcmgr = PDFResourceManager()
    retstr = io.StringIO()
    print(type(retstr))
    codec = 'utf-8'
    laparams = LAParams()
    device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)

    page_no = 0
    pageData = []
    for pageNumber, page in enumerate(PDFPage.get_pages(fp)):
        if pageNumber == page_no:
            interpreter.process_page(page)

            data = retstr.getvalue()

            # with open(os.path.join('Files/Company_list/0010/text_parsed/2017AR', f'pdf page {page_no}.txt'),
            #           'wb') as file:
            pageData.append({'pageNumber' : pageNumber , 'text':data.encode('utf-8')})
            data = ''
            retstr.truncate(0)
            retstr.seek(0)

        page_no += 1

#    text =

    fp.close()
    device.close()
    retstr.close()
    return pageData


def read_pdf(file):

    file = open("C:\\1\\sample.pdf", 'rb')
    read_pdf = PyPDF2.PdfFileReader(file)
    number_of_pages = read_pdf.getNumPages()
    print(number_of_pages)
    c = collections.Counter(range(number_of_pages))
    for i in c:
        page = read_pdf.getPage(i)
        page_content = page.extractText()
        print(page_content.encode('utf-8'))

if _name_ == '__main__':
    app.config['JSON_AS_ASCII'] = False
    app.config['JSON_SORT_KEYS'] = False
    #app.run('127.0.0.1', port=5016)
    app.run(debug=True)