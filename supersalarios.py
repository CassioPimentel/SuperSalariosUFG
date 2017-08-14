# -*- coding: utf-8 -*-
import threading
import time
import os
import urllib2
from bs4 import BeautifulSoup

# Máximo de conexões/threads simultâneas
MAX_CONEXOES = 11

def RaspUFG(pag, nArq):
    
    arq = open('UFG-ValorMensal'+str(nArq)+'.txt', 'w')
    
    #For para percorrer todas 374 paginas com os servidores UFG
    for j in range(pag,min(374,pag+100)):
        #Criacao da "sopa" com os dados da pagina atual do poral de transparencia da UFG
        html = urllib2.urlopen('http://www.portaldatransparencia.gov.br/servidores/OrgaoLotacao-ListaServidores.asp?CodOS=15000&DescOS=MINISTERIO%20DA%20EDUCACAO&CodOrg=26235&DescOrg=UNIVERSIDADE%20FEDERAL%20DE%20GOIAS&Pagina='+str(j))
        bsObj = BeautifulSoup(html, "html.parser")
    
        #Obtencao dos elementos da tabela com os dados dos servidores
        tabela = bsObj.find("table",{"summary":"Lista de servidores lotados por órgão"})
        
        teto = 20000.00
        
        #Obtencao dos dados dos elementos da tabela
        if tabela != None:
            tds = tabela.findAll("a")
            for link in tds:
                link2 = link.get('href')
                nome = link.contents[0]
                idServidor = link2[44:51]
                
                if idServidor != "":
                    html = urllib2.urlopen('http://www.portaldatransparencia.gov.br/servidores/Servidor-DetalhaRemuneracao.asp?Op=3&IdServidor='+str(idServidor)+'&CodOS=15000&CodOrgao=26235&bInformacaoFinanceira=True')
                    bsObj = BeautifulSoup(html, "html.parser")
                
                    #Obtencao dos elementos da tabela com os dados dos servidores
                    salario = bsObj.find("tr",{"class":"remuneracaolinhatotalliquida"})
                    if salario != None:
                        salarioValor = salario.find("td",{"class":"colunaValor"}).get_text()
                    else:
                        salarioValor =  ""
                    
                    textoVM = []
                    
                    if salarioValor != "":
                        if(float(salarioValor.strip().replace('.','').replace(',','.'))>=teto):
                            print nome
                            print salarioValor
                            
                            textoVM.append('Nome:'+ nome + '\n')
                            textoVM.append('Salario:'+ salarioValor + '\n')
                            arq.writelines(textoVM)
    
        #Limpa a "sopa"
        bsObj.clear()


# Thread principal
lista_threads = []
for k in range(MAX_CONEXOES-2):
    while threading.active_count() > MAX_CONEXOES:
        #mostrar_msg("Esperando 1s...")
        time.sleep(1)
    thread = threading.Thread(target=RaspUFG, args=((100*k)+1,k+1,))
    lista_threads.append(thread)
    thread.start()
 
# Esperando pelas threads abertas terminarem
#mostrar_msg("Esperando threads abertas terminarem...")
for thread in lista_threads:
    thread.join()

#Rotina para junção dos arquivos gerados pelas threads em um unico arquivo
for m in range(1,MAX_CONEXOES-1):
    #Seleção de um dos arquivos das threads
    arqui = (open('UFG-ValorMensal'+str(m)+'.txt', 'r'))
    path =  os.path.dirname(os.path.realpath(__file__))
    dir = os.listdir(path)
    arquivo = open('UFG-ValorMensal.txt', 'a')
    #For para escrever os dados do arquivo selecionado no novo arquivo
    for line in arqui:
        arquivo.writelines(line)
    #For para apagar os arquivos gerados pelas threads
    for arqu in dir:
        if arqu == 'UFG-ValorMensal'+str(m)+'.txt':
            arqui.close()
            os.remove(arqu)

print "FIM"