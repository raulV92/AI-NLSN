##Version 0.3 (básica )descargable
##
## re-made proyect on grafical shit...

import numpy as np 
import pandas as pd 
import sqlite3
import re

import os
from pathlib import Path

import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox


### funciones de GUI
def ubicacion():
    texto = filedialog.askdirectory()
    inTxt=carpeta.get()
    carpeta.delete(0,len(inTxt))
    carpeta.insert(0,texto)

def mensage(txt):
    messagebox.showinfo('Process Completed',txt)

### funciones de ejecución
def errorCase(txt):
    messagebox.showerror('Operation Failed',txt+'\n Please correct & run again...')
    output1.insert(4,'NOT_OK!!')


#######################################
## define history Val  #####
#####################################
def historyVal(historyDB,valueReport,colsReport,colsDetails,valueDetails):
    try:
        hcdar=pd.read_csv(Path(rawPath,'historyCDAR.csv'),error_bad_lines=False,encoding='utf_16_le',sep='\t')  ##history val
        #hcdar=pd.read_csv('historyTest.csv')
    except:
        errorCase("Could not open 'hitoryCDAR.csv' file,\n Encoding required = UTF-16-LE ")

    hcdar=pd.read_csv('historyCDAR.csv',error_bad_lines=False,encoding='utf_16_le',sep='\t')  ##history val

    #historia en arcivo SQLite
    c1.execute('SELECT Audit_Id, Entity_Id, Fact_Id, PreviousValue From HistoryFact')
    historyDB=pd.DataFrame(c1.fetchall())
    hDBcols=['Audit_Id', 'Entity_Id', 'Fact_Id','PreviousValue']
    historyDB.columns=hDBcols
    #tabSQL===  	ShopCode	Shop_Id	Audit_Id	Frequency	Value	SMS_Fr
    historyDB=pd.merge(historyDB,tabSQL[['ShopCode','Audit_Id']], on='Audit_Id')

    def setPreValue(row):
        if row['PreviousValue']=='True':
            return np.float64(1)
        elif row['PreviousValue']=='False':
            return np.float64(0)
        elif row['PreviousValue']=='':
            return np.nan
        else:
            return np.float64(row['PreviousValue'])

    historyDB['PreviousValue']=historyDB.apply(setPreValue,axis=1)
    historyDB=historyDB[np.isnan(historyDB['PreviousValue'])==False]
    historyDB=historyDB.astype({'ShopCode':'int64'})

    storesToChk=pd.merge(hcdar['SMS Id'].drop_duplicates(),historyDB['ShopCode'].drop_duplicates(),left_on='SMS Id',
                        right_on='ShopCode',how='outer',indicator=True)
    storesNotFound=storesToChk.ShopCode[storesToChk['_merge']=='right_only'].drop_duplicates() ## array
    storesFounded=storesToChk.ShopCode[storesToChk['_merge']=='both'].drop_duplicates()
    if len(storesNotFound)>0:
        notFoundCdar1=historyDB[historyDB['ShopCode'].isin(storesNotFound)]
        historyDB=historyDB[historyDB['ShopCode'].isin(storesFounded)]
    else:
        notFoundCdar1=pd.DataFrame([],columns=['Audit_Id','Entity_Id','Fact_Id','PreviousValue','ShopCode'])
        
    hcdar=hcdar[hcdar['SMS Id'].isin(historyDB['ShopCode'].drop_duplicates())].copy()####  filtro 1

    def causalJoin(row):
        if not np.isnan(row['Fact Attribute Index']):
            return np.int64(row['Fact Id']*10**5+row['Fact Attribute Index'])
        else: 
            return  np.int64(row['Fact Id'])

    hcdar['Fact Id']=hcdar.apply(causalJoin,axis=1) ##axis =1 aplica sobre renglon
    hcdar=hcdar[['Entity Id','Fact Id','Value','SMS Id']]
    #####  PARTE 2
    colsEq={'Entity Id':'Entity_Id',
            'Fact Id':'Fact_Id',
            'Value':'PreviousValue',
            'SMS Id':'ShopCode'}
    hcdar.rename(columns=colsEq, inplace= True)
    #interseccion=pd.merge(hcdar,historyDB, how='inner')
    result_H=pd.merge(historyDB,hcdar,how='outer',indicator=True)
    if len(result_H[result_H._merge=='both'])==len(historyDB):
        valueReport.append(['History Validation','Pass'])
    else:
        valueReport.append(['History Validation','NotPass'])
        acWrong=pd.DataFrame([],['Audit_Id_x','Entity_Id','Fact_Id','PreviousValue','ShopCode','Audit_Id_y','trueValue','_merge'])
        acNotFound=pd.DataFrame([],columns=['Frequency','Audit_Id','Entity_Id','Fact_Id','PreviousValue','ShopCode','_merge'])
        for st in storesFounded:
            auxHCDAR=hcdar[hcdar.ShopCode==st]
            auxDB=historyDB[historyDB.ShopCode==st]
            result=pd.merge(auxDB,auxHCDAR,how='outer',indicator=True)
            #correct=result[result._merge=='both']
            justSQL=result[result._merge=='left_only']
            #justSQL=justSQL._merge.map({'left_only':'Just in collector'})
            justCDAR=result[result._merge=='right_only']
            #justCDAR=justCDAR._merge.map({'right_only':'Just in Collected Data Rep'})

            wrongValues=justSQL.copy()
            trueValues=justCDAR[(justCDAR['Fact_Id'].isin(justSQL['Fact_Id'].unique()) ) &
                                (justCDAR['Entity_Id'].isin(justSQL['Entity_Id'].unique()) )]
            notFoundValues=pd.merge(justSQL,trueValues,how='left')
            notFoundValues=pd.merge(notFoundValues,tabSQL[['Frequency','Audit_Id']],on='Audit_Id',sort=True)
            acNotFound=pd.concat([acNotFound,notFoundValues],axis=0,sort=True)

            wrongValues.rename(columns={'PreviousValue':'colectorValue'},inplace=True)#.drop(columns=['_merge'])
            trueValues.rename(columns={'PreviousValue':'reportValue'},inplace=True)#.drop(columns=['_merge'])
            wrongValues=wrongValues.drop(columns=['_merge'])
            trueValues=trueValues.drop(columns=['_merge'])

            outRep=pd.merge(wrongValues,trueValues, on=['Entity_Id','Fact_Id','ShopCode'],how='outer')
            acWrong=pd.concat([acWrong,outRep],axis=0,sort=True)
            #n=n+1

        
        outRep.to_csv(Path(rawPath,resFolder,'valuesHistory.csv'))
        acNotFound._merge=acNotFound._merge.map({'left_only':'Just in Collector','right_only':'Just in Collected Data Rep'})
        acNotFound.to_csv(Path(rawPath,resFolder,'valuesNotFoundHist.csv'))
#########

def principal():
    rawPath = carpeta.get()
    rawPath=Path(rawPath)
    os.chdir(rawPath)

    resFolder='ValidationResult'
    try:
        os.makedirs(Path(rawPath,resFolder))  ### TODO: hacer que borre la carpeta y crearla otra vez
    except:
        pass
    colsReport=['Description','Results:']
    valueReport=[]
    colsDetails=['item','values compared','values downloaded']
    valueDetails=[]

    ##conecta a sqlite principal
    
    try:
        data_base= sqlite3.connect(Path(rawPath,'AI.sqlite'))
    except:
        errorCase("Could not connect to 'AI.sqlite' file")
    else:
        c1=data_base.cursor()

    auditor=auditorBox.get()

    try:  ##################### Validacion de plan
        plan=pd.read_excel(Path(rawPath,"plan.xls")) 
        plan=plan[plan['Type']=='ASSIGNED']
        plan=plan[['SMS ID','Activity Local Name','Associate Cdar ID','Associate','Frequency']]
        plan=plan.astype({'SMS ID':'int64','Associate Cdar ID':'int64'})
        print(plan.dtypes)
    #except:
    #    plan=pd.read_csv(Path(rawPath,"plan.csv",encoding='ANSI'))
    except:
        errorCase("Could not open 'plan.csv' file,\n Encoding required = UTF-8 ")
        ## hacer que intente abrir con ANSI
    if 'SMS ID' not in plan.columns:
        errorCase("Cannot find 'SMS ID' column in plan.csv file.")
    if 'Activity Local Name' not in plan.columns:
        errorCase("Cannot find 'Activity Local Name' column in plan.csv file.")
    if 'Associate Cdar ID' not in plan.columns:
        errorCase("Cannot find 'Associate Cdar ID' column in plan.csv file.")


    if auditor not in plan['Associate'].unique():
        errorCase("Cannot find Auditor name in plan.csv file.")

    else:
        planAuditor=plan[plan['Associate']==auditor]
        auditorID=planAuditor['Associate Cdar ID'].unique()[0]
        planAuditor=planAuditor[['SMS ID','Frequency','Activity Local Name']].drop_duplicates().copy()
        auditorSQL=pd.read_sql_query("SELECT Auditor_Id FROM 'Auditor';",data_base)  

        if auditorID not in auditorSQL.Auditor_Id.unique():
            errorCase("Cannot find AuditorID name in AI.sqlite file.")
        
    planAuditor['Frequency']=planAuditor['Frequency'].map({'Monthly': 'M',
                                                        'Monthly Inad':'MI',
                                                        'Bi-Monthly_Drug': 'BM_D',
                                                        'Bi-Monthly_Food':'BM_F'})

    planAuditor=planAuditor.astype({'SMS ID':'int64'})
    #planAuditor[['SMS ID','Frequency','Activity Local Name']].sort_values(by=['SMS ID'])
    planAuditor['SMS_Fr']=planAuditor['SMS ID'].astype(str)+planAuditor['Frequency']
    auditTab=pd.read_sql_query("SELECT Audit_Id, Shop_Id, Period, Frequency FROM 'Audit';",data_base)
    shopTab=pd.read_sql_query("SELECT Shop_Id, ShopCode, IndexCode FROM 'Shop';",data_base)
    shop_Idis=pd.merge(shopTab[['ShopCode','Shop_Id']],auditTab[['Audit_Id','Shop_Id','Frequency']],
                    how='outer',on='Shop_Id',indicator=True)

    BOinst=pd.read_sql_query("SELECT Audit_Id, Value FROM 'BackOfficeInstruction';",data_base) 
    tabSQL=pd.merge(shop_Idis,BOinst,how='outer',on=['Audit_Id']).dropna()
    tabSQL=tabSQL.drop(columns='_merge')
    tabSQL['SMS_Fr']=tabSQL['ShopCode']+tabSQL['Frequency']

    #####
    #####  Consideron alternatives Patch...  (filtra por ShopCode)

    alterna= pd.read_sql_query('''select Shop.ShopCode from shop
                                join Audit on shop.Shop_Id==Audit.Shop_Id
                                where Audit.Audit_Id 
                                in (select Audit_Id from TemplateAudit where Sequence <>1);''',data_base )    

    tabSQL=tabSQL[~tabSQL['ShopCode'].isin(alterna.ShopCode.tolist())].copy()
    
    ##### End Of Alternatives Patch ...

    result1=pd.merge(tabSQL[['ShopCode','Frequency','SMS_Fr']],
                    planAuditor[['SMS ID','Frequency','SMS_Fr']],on='SMS_Fr',indicator=True,how='outer')
    result1['_merge']=result1['_merge'].map({'both': 'correct',
                                            'left_only': 'Just in Colector','right_only':'Just in plan'})




    valueReport.append(['Periods downloaded in Audit Instructions',auditTab['Period'].unique()])
    ## Tendas para actividades e indices
    SMScorrect=result1['SMS_Fr'][result1['_merge']=='correct'].unique() 

    ## ni idea por que pero functiona....
    result1=result1.drop_duplicates()  # <--------------


    if len(result1[result1['_merge'] != 'correct'])==0:
        valueReport.append(['Verify If stores in plan are in Collector','Pass'])
        valueReport.append(['Number of Stores Downloaded',len(result1)])
    else:
        valueReport.append(['Verify If stores in plan are in Collector','NOT.Pass'])
        details1=result1[result1['_merge']!='correct']
        details1.to_csv(Path(rawPath,resFolder,'storesDetails.csv'))  

    #####################################  
    ####### VALIDACION DE ACTIVIDADES  ##
    def planStd(plan):
        planSinDup=plan[plan['SMS_Fr'].duplicated(keep=False).map(lambda x: not x)]
        if len(planSinDup) == len(plan):
            return plan
        else:
            
            planDup=plan[plan['SMS_Fr'].duplicated(keep=False)]
            repeatTab=[]
        
            for smsFr in planDup['SMS_Fr'].unique():
                stTab=planDup[planDup.SMS_Fr==smsFr]
                #display(stTab)
                repeatTab.append([stTab['SMS ID'].unique()[0]                     , stTab.Frequency.unique()[0],
                                ';'.join(stTab['Activity Local Name'].tolist()) , stTab.SMS_Fr.unique()[0]])
                #print([stTab['SMS ID'].unique()[0],stTab.Frequency.unique()[0],stTab['Activity Local Name'].tolist(),stTab.SMS_Fr.unique()[0]])
            repeatTab=pd.DataFrame(repeatTab,columns=['SMS ID','Frequency','Activity Local Name','SMS_Fr'])
            return pd.concat([planSinDup,repeatTab])

    planAuditor=planStd(planAuditor)  ### <- llama funcion que estandariza el plan!!!

    tabSQL=tabSQL[tabSQL['SMS_Fr'].isin(SMScorrect)]
    tabAct=pd.merge(planAuditor,tabSQL,on='SMS_Fr')
    def algo(row):
        Asql=row['Value'].split(',')
        Aplan=row['Activity Local Name'].split(';')
        if set(Asql)==set(Aplan):
            return True
        else:
            return False
        
    tabAct['result']=tabAct.apply(algo,axis=1)

    if False in tabAct.result.unique():
        valueReport.append(['Verify activities in stores ','NotPass'])
        #tabAct=tabAct.drop(columns)
        result2=tabAct[tabAct['result']==False]
        result2=result2.rename(columns={'Activity Local Name':'Activity in Plan',
                                        'Value':'Activity in Colector'})
        result2[['SMS ID','Activity in Plan','Activity in Colector']].to_csv(Path(rawPath,resFolder,'actDetails.csv'))
        mensage("file generated : 'actDetails.csv'")
    else:
        valueReport.append(['Verify activities in stores ','Pass'])

    #####################################
    ######## INDEX VALIDATION  ##########    
    try:
        gsr=pd.read_csv("GSR.csv",encoding='utf_16_LE',sep='\t')  #####################################
    except:
        try: 
            preSync=pd.read('preSync.csv')
        except:
            errorCase("Could not open 'GSR.csv' nor 'preSync.csv'(Brazil) file,\n Encoding required = UTF-16-LE")
        
        else:
            pass

    if 'SMS ID' not in gsr.columns:
        errorCase("Cannot find 'SMS ID' column in GSR.csv file.")
    #indice=re.compile(r'\d\d')
    indice=re.compile(r'^\d+') ## el titulo de la columna debe comenzar por el indice
    ind=0
    for titulo in gsr.columns:
        res=indice.search(titulo)
        if res!=None:
            ind=ind+1
    if ind==0:
        errorCase("Cannot find Index columns in GSR.csv file.")
        

    SMSandIndex= shopTab[['ShopCode','IndexCode']].drop_duplicates()
    SMSandIndex['SMS ID']=shopTab['ShopCode']  ## Renombrar al pasar a .py
    SMSandIndex=SMSandIndex.astype({'SMS ID':'int64'})

    lojas=pd.merge(SMSandIndex,gsr,how='outer', on='SMS ID',indicator=True)
    lojasIndex=lojas[lojas['_merge']=='both']

    def indexEnVct(row):
        #indice=re.compile(r'\d\d')
        indice=re.compile(r'^\d+')
        index_gsr=[]
        for header in row.index:
            res=indice.search(header)
            if res!=None:
                if row.loc[header]==1:
                    index_gsr.append(int(indice.search(header).group()))
                    
            #print(row.loc[ind])
        #print('-----',row.loc['ShopCode'],'-----------')
        gsr=index_gsr
        sql=list(map(int,(row.loc['IndexCode'].split(','))))
        return [row.loc['ShopCode'],gsr,sql]

    result3=lojasIndex.apply(indexEnVct,axis=1)  ## variable 'result3' es Series
    result3=pd.DataFrame(result3.tolist(),columns=['Shop','Index in GSR','Index in Colector'])
    def valIndex(row):
        if set(row['Index in GSR'])==set(row['Index in Colector']):
            return True
        else:
            return False
    result3['Validated']=result3.apply(valIndex,axis=1)

    if len(result3[result3['Validated']==False])==0:
        valueReport.append(['Index Validation per Store','Pass'])
    else:
        valueReport.append(['Index Validation per Store','NotPass'])
        result3[result3.Validated==False].to_csv(Path(rawPath , resFolder,'IndexDetails.csv'))
        mensage("File generates: 'IndexDetails.csv' ")

    ### llama a validacion de historia

    tara = messagebox.askokcancel("Question","continue History Validation??")
    if tara==True:

        try:
            hcdar=pd.read_csv(Path(rawPath,'CollectedData.csv'),error_bad_lines=False,encoding='utf_16_le',sep='\t')  ##history val
            #hcdar=pd.read_csv('historyTest.csv')
        except:
            errorCase("Could not open 'CollectedData.csv' file,\n Encoding required = UTF-16-LE ")

        #hcdar=pd.read_csv('CollectedData.csv',error_bad_lines=False,encoding='utf_16_le',sep='\t')  ##history val

        #historia en arcivo SQLite
        c1.execute('SELECT Audit_Id, Entity_Id, Fact_Id, PreviousValue From HistoryFact')
        historyDB=pd.DataFrame(c1.fetchall())
        hDBcols=['Audit_Id', 'Entity_Id', 'Fact_Id','PreviousValue']
        historyDB.columns=hDBcols
        #tabSQL===  	ShopCode	Shop_Id	Audit_Id	Frequency	Value	SMS_Fr
        historyDB=pd.merge(historyDB,tabSQL[['ShopCode','Audit_Id']], on='Audit_Id')

        def setPreValue(row):
            if row['PreviousValue']=='True':
                return np.float64(1)
            elif row['PreviousValue']=='False':
                return np.float64(0)
            elif row['PreviousValue']=='':
                return np.nan
            else:
                return np.float64(row['PreviousValue'])

        historyDB['PreviousValue']=historyDB.apply(setPreValue,axis=1)
        historyDB=historyDB[np.isnan(historyDB['PreviousValue'])==False]
        historyDB=historyDB.astype({'ShopCode':'int64'})

        storesToChk=pd.merge(hcdar['SMS Id'].drop_duplicates(),historyDB['ShopCode'].drop_duplicates(),left_on='SMS Id',
                            right_on='ShopCode',how='outer',indicator=True)
        storesNotFound=storesToChk.ShopCode[storesToChk['_merge']=='right_only'].drop_duplicates() ## array
        storesFounded=storesToChk.ShopCode[storesToChk['_merge']=='both'].drop_duplicates()
        if len(storesNotFound)>0:
            notFoundCdar1=historyDB[historyDB['ShopCode'].isin(storesNotFound)]
            historyDB=historyDB[historyDB['ShopCode'].isin(storesFounded)]
        else:
            notFoundCdar1=pd.DataFrame([],columns=['Audit_Id','Entity_Id','Fact_Id','PreviousValue','ShopCode'])
            
        hcdar=hcdar[hcdar['SMS Id'].isin(historyDB['ShopCode'].drop_duplicates())].copy()####  filtro 1

        def causalJoin(row):
            if not np.isnan(row['Fact Attribute Index']):
                return np.int64(row['Fact Id']*10**5+row['Fact Attribute Index'])
            else: 
                return  np.int64(row['Fact Id'])

        hcdar['Fact Id']=hcdar.apply(causalJoin,axis=1) ##axis =1 aplica sobre renglon
        hcdar=hcdar[['Entity Id','Fact Id','Value','SMS Id']]
        #####  PARTE 2
        colsEq={'Entity Id':'Entity_Id',
                'Fact Id':'Fact_Id',
                'Value':'PreviousValue',
                'SMS Id':'ShopCode'}
        hcdar.rename(columns=colsEq, inplace= True)
        #interseccion=pd.merge(hcdar,historyDB, how='inner')
        result_H=pd.merge(historyDB,hcdar,how='outer',indicator=True)
        if len(result_H[result_H._merge=='both'])==len(historyDB):
            valueReport.append(['History Validation','Pass'])
        else:
            valueReport.append(['History Validation','NotPass'])
            acWrong=pd.DataFrame([],['Audit_Id_x','Entity_Id','Fact_Id','PreviousValue','ShopCode','Audit_Id_y','trueValue','_merge'])
            acNotFound=pd.DataFrame([],columns=['Frequency','Audit_Id','Entity_Id','Fact_Id','PreviousValue','ShopCode','_merge'])
            for st in storesFounded:
                auxHCDAR=hcdar[hcdar.ShopCode==st]
                auxDB=historyDB[historyDB.ShopCode==st]
                result=pd.merge(auxDB,auxHCDAR,how='outer',indicator=True)
                #correct=result[result._merge=='both']
                justSQL=result[result._merge=='left_only']
                #justSQL=justSQL._merge.map({'left_only':'Just in collector'})
                justCDAR=result[result._merge=='right_only']
                #justCDAR=justCDAR._merge.map({'right_only':'Just in Collected Data Rep'})

                wrongValues=justSQL.copy()
                trueValues=justCDAR[(justCDAR['Fact_Id'].isin(justSQL['Fact_Id'].unique()) ) &
                                    (justCDAR['Entity_Id'].isin(justSQL['Entity_Id'].unique()) )]
                notFoundValues=pd.merge(justSQL,trueValues,how='left')
                notFoundValues=pd.merge(notFoundValues,tabSQL[['Frequency','Audit_Id']],on='Audit_Id',sort=True)
                acNotFound=pd.concat([acNotFound,notFoundValues],axis=0,sort=True)

                wrongValues.rename(columns={'PreviousValue':'colectorValue'},inplace=True)#.drop(columns=['_merge'])
                trueValues.rename(columns={'PreviousValue':'reportValue'},inplace=True)#.drop(columns=['_merge'])
                wrongValues=wrongValues.drop(columns=['_merge'])
                trueValues=trueValues.drop(columns=['_merge'])

                outRep=pd.merge(wrongValues,trueValues, on=['Entity_Id','Fact_Id','ShopCode'],how='outer')
                acWrong=pd.concat([acWrong,outRep],axis=0,sort=True)
                #n=n+1

            
            outRep.to_csv(Path(rawPath,resFolder,'valuesHistory.csv'))
            acNotFound._merge=acNotFound._merge.map({'left_only':'Just in Collector','right_only':'Just in Collected Data Rep'})
            acNotFound.to_csv(Path(rawPath,resFolder,'valuesNotFoundHist.csv'))

##########################################################
##########################################################
##########################################################



#####################################################
#####################################################
#####################################################
    
    output1.insert(4,'OK!!')  ## termino de programa
    
    ###### output y cierre a DB
    valReport=pd.DataFrame(valueReport,columns=colsReport)
    data_base.close()
    valReport.to_csv(Path(rawPath,resFolder,'valReport.csv'))
    mensage("File Generated: 'valReport.csv' \nEnd of validation")
    #print('Tiempo de ejecucion:', datetime.now()-startTime)
    #input('Validacion Filanizada... ')



###########  GUI SET UP
root=tk.Tk()
root.title('AI Validation Ver 0.3')

line1=tk.Label(root, text="THIS PROGRAM VALIDATES DE AI FILES DOWNLOADED TO COLLECTOR\n")
#line1.grid(column=1,row=1)
line1.pack()
line2=tk.Label(root, text="1) Insert the path to input files...")
#line2.grid(column=1,row=2)
line2.pack()
carpeta=tk.Entry(root, width=50)
#carpeta.grid(column=1,row=3)
carpeta.pack()
toBrowse=tk.Button(root,text='Browse...',command=ubicacion)
#toBrowse.grid(column=2,row=3)
toBrowse.pack()
line4=tk.Label(root, text="2) Write Auditor name and RUN")
line4.pack()

auditorBox=tk.Entry(root, width=40)
auditorBox.pack()

corre=tk.Button(root,text='RUN',command=principal)
corre.pack()

output1=tk.Entry(root,width=12,borderwidth=7)
output1.pack()

root.mainloop()
###########################

