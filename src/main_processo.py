#!/usr/bin/env python
import sys
import re
import funcoes_transversais as func


auxREFDATE = re.split('-', REFDATE)
REFDATE_modified = "{}{}".format(auxREFDATE[0],auxREFDATE[1])

def propertiesFileInformationProcesso():
	#----- Input Variables
	
	global variavel1
	variavel1 = main.propertiesFileParser.get("INPUTS", "VARIAVEL1").strip()



def funcao1():
    
    query = "SELECT * FROM table"
    
    func.drop_create_table("tabela", query)
	return


def funcao2():
	return
	



class process_processo(object):

	def __init__(self, propProcesso):
		main.loadPropertiesFile(propProcesso)
		propertiesFileInformationUni()

	def start(self):
		
		LOGGER.info( "\n INICIO: PROCESSO \n" )
		
				
		
		
		funcao1()
		funcao2()
		
			
		main.setSubProcStatusFromTableName("Processo", hProcesso, "concluido")
		
		
		LOGGER.info(" \n FIM: UNIVERSO \n")
			
		return
