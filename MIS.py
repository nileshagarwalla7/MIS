from pyspark import SparkConf,SparkContext
import time
import sys
import re
from pyspark.sql.functions import *
from pyspark.sql.functions import broadcast
from pyspark.sql import *
from pyspark.sql.types import *
import datetime
from pyspark.sql import functions as F
import pandas as pd
from time import gmtime, strftime
import logging
import argparse
from pytz import timezone
import pytz
from datetime import datetime
from pyspark.sql.types import MapType
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
import smtplib
from email import encoders

### creating spark context
conf = SparkConf()
conf.setAppName('alpha-code')
sc = SparkContext(conf=conf)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


def get_ist_date(utc_dt):
    import pytz
    from datetime import datetime
    local_tz = pytz.timezone('Asia/Kolkata')
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_tz.normalize(local_dt)

def get_pst_date():
    from datetime import datetime
    dt = datetime.utcnow()
    return dt

def lower_locale(locale):
    return locale.lower()

lower_locale_udf = udf(lower_locale,StringType())

#PST date for comparing with IST
def get_pst_date_final(utc_dt):
    import pytz
    from datetime import datetime
    local_tz = pytz.timezone('US/Pacific')
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_tz.normalize(local_dt)
    
from datetime import datetime as dt

StartDate = get_pst_date()

### Function for importing SQL table
def importSQLTable(dbname, tablename):
    table = (sqlContext.read.format("jdbc")
    .option("url", severName.value["mysql"])
    .option("driver",severProperties.value["mysql"]["driver"])
    .option("dbtable", dbname+".dbo."+tablename)
    .option("user", severProperties.value["mysql"]["user"])
    .option("password", severProperties.value["mysql"]["password"]).load())
    return table

def importAURORATable(dbname, tablename):
    #Function to import tables from Aurora database
    table = (sqlContext.read.format("jdbc")
    .option("url", severName.value["Aurora"])
    .option("driver",severProperties.value["Aurora"]["driver"])
    .option("dbtable", dbname+"."+tablename)
    .option("user", severProperties.value["Aurora"]["user"])
    .option("password", severProperties.value["Aurora"]["password"]).load())
    return table

def importSQLTableForBusinessBasedSuppression(query,url):
    table = (sqlContext.read.format("jdbc").
        option("url", url).
        option("driver",severProperties.value["mysql"]["driver"]).
        option("dbtable", query).
        option("user", severProperties.value["mysql"]["user"]).
        option("password", severProperties.value["mysql"]["password"]).load())
    return table

AlphaStartDate = get_pst_date()
print("AlphaStartDate = ",AlphaStartDate)

### defining log id initiation
rep = 1440

### function to append logs to central log table

def log_df_update(spark,IsComplete,Status,EndDate,ErrorMessage,RowCounts,StartDate,FilePath,tablename):
    import pandas as pd
    l = [(process_name_log.value,process_id_log.value,IsComplete,Status,StartDate,EndDate,ErrorMessage,int(RowCounts),FilePath)]
    schema = (StructType([StructField("SourceName", StringType(), True),StructField("SourceID", IntegerType(), True),StructField("IsComplete", IntegerType(), True),StructField("Status", StringType(), True),StructField("StartDate", TimestampType(), True),StructField("EndDate", TimestampType(), True),StructField("ErrorMessage", StringType(), True),StructField("RowCounts", IntegerType(), True),StructField("FilePath", StringType(), True)]))
    rdd_l = sc.parallelize(l)
    log_df = spark.createDataFrame(rdd_l,schema)
    #  "Orchestration.dbo.AlphaProcessDetailsLog_LTS"
    log_df.withColumn("StartDate",from_utc_timestamp(log_df.StartDate,"PST")).withColumn("EndDate",from_utc_timestamp(log_df.EndDate,"PST")).write.jdbc(url=url['mysql'], table=tablename,mode="append", properties=properties['mysql'])



AlphaLaunchDate = get_pst_date()
ISTdate= get_ist_date(dt.utcnow())
PSTdate = get_pst_date_final(dt.utcnow())
ISTLauchDate=ISTdate.strftime('%Y-%m-%d')
PSTLauchDate=PSTdate.strftime('%Y-%m-%d')

#print("AlphaLaunchDate: ",AlphaLaunchDate)
#print("ISTdate: ",ISTdate)
#print("PSTdate: ", PSTdate)
#print("ISTLauchDate: ",ISTLauchDate)
#print("PSTLauchDate: ",PSTLauchDate)

if((ISTdate > PSTdate) & (dt.now(pytz.timezone('US/Pacific')).hour > 20)):
    launch_dt = ISTLauchDate
else:
    launch_dt = PSTLauchDate

### defining parameters for a campaign 
StartDate = get_pst_date()

try :

    parser = argparse.ArgumentParser()
    parser.add_argument("--locale_name", help="Write locale_name like en_nz. This is manadatory.")
    parser.add_argument("--job_type", help="Write prod or test. This is manadatory. Used to determine type of job")
    parser.add_argument("--env_type", help="Write prod or test. This is manadatory. Used to determine data source")
    parser.add_argument("--cpgn_type", help="Write loyalty or marketing")
    parser.add_argument("--run_date", help="Write launch_date like Y-M-D")
    parser.add_argument("--test_type", help="AUTOTEST or MANUAL or BACKUP OR XYZ")
    parser.add_argument("--campaign_id", help="Campaign ID or Null")
    parser.add_argument("--email_address", help="Enter email address")
 #   parser.add_argument("--GITVER", help="Enter git version to use")

    args = parser.parse_args()
    locale_name = args.locale_name
    job_type = args.job_type
    env_type = args.env_type
    cpgn_type = args.cpgn_type
    run_date = args.run_date
    test_type = args.test_type
    locale = locale_name
 #   GITVER = args.GITVER
    
#   locale = 'en_ca'
#   job_type = 'test'
#   env_type = 'prod'
    
#   if job_type == 'test':
#       run_date = '2017-12-07'
#       test_type = 'BACKUP'
#       
    email_address = ''
    campaign_id = ''    

    if job_type == 'test':
        if test_type == 'MANUAL':
            email_address = args.email_address
            campaign_id = args.campaign_id
        elif test_type == 'AUTOTEST':
            email_address = args.email_address
    
    #locale = 'en_nz' #this will have to be commented out during the migration to jenkins
    #job_type = 'test' #this variable will always be prod for Jenkin jobs
    #env_type = 'test'
    
    #run_date = '2017-10-29'
    #test_type = 'BACKUP'
    
    #campaign_id = 1
    #email_address = 'xxx@expedia.com'
    
    ### sql server info for writing log table
    properties = {"mysql"   : {"user" : "occuser", "password":"Exp3dia22", "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"},
                  "Aurora"  : {"user":"occauroradbread", "password":"occ@uroradbread", "driver":"com.mysql.jdbc.Driver"}}
                  
    url = {"mysql"  : "jdbc:sqlserver://10.23.18.135",
           "Aurora" : "jdbc:mysql://occ-eml-prod-cluster.cluster-ro-cisrflibsho6.us-east-1.rds.amazonaws.com:3306",
           "BusinessBasedSuppression" : "jdbc:sqlserver://10.23.19.63"}
    ocelotdb="AlphaProd"
    if env_type != 'prod':
        url["mysql"] = "jdbc:sqlserver://10.23.16.35"
        ocelotdb="AlphaTest"
    
    severName = sc.broadcast(url)
    severProperties = sc.broadcast(properties)
    
    #hard-coded variables
    data_environ = env_type ##for production it should be always 'prod'
    pos = locale.split('_')[1].upper()
    current_date =  time.strftime("%Y/%m/%d")
    
    status_table = importSQLTable("Orchestration","AlphaConfig")
    pos1 = locale.split('_')[1].upper() #Converting the country to uppercase
    pos0 = locale.split('_')[0]  #Obtaining the language code
    
    global search_string
    search_string = pos0+"_"+pos1 #storing locale in 'en_US' format
    required_row = status_table.filter(status_table.Locale == search_string).filter("brand like 'Brand Expedia'").collect() #finding out the rows that contain the brand Expedia only
  
    global process_id
    global process_name 
    process_id = required_row[0]['id']  #ID of the particular row
    process_name = required_row[0]['ProcessName'] #Process name for the filtered row
    
    global mis_data_rdd_dic
    mis_data_rdd_dic = {}

    process_id_log = sc.broadcast(process_id)
    process_name_log = sc.broadcast(process_name)
    
    if job_type == 'prod':
        CentralLog_str='Orchestration.dbo.CentralLog'
        AlphaProcessDetailsLog_str='Orchestration.dbo.AlphaProcessDetailsLog'
        success_str='Campaign Suppression process completed'
        failure_str='Campaign Suppression process failed'
    elif job_type == 'test':
        if test_type != 'BACKUP':
            CentralLog_str='Orchestration.dbo.CentralLog_LTS'
            AlphaProcessDetailsLog_str='Orchestration.dbo.AlphaProcessDetailsLog_LTS'
            success_str='Live Test Sends completed'
            failure_str='Live Test Sends failed'
        else:
            CentralLog_str='Orchestration.dbo.CentralLog_BACKUP'
            AlphaProcessDetailsLog_str='Orchestration.dbo.AlphaProcessDetailsLog_BACKUP'
            success_str='Campaign Suppression process completed'
            failure_str='Campaign Suppression process failed'
    
    log_df_update(sqlContext,1,"MIS module has started for {} ".format(locale_name),get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)
    log_df_update(sqlContext,1,'Parameters are correct',get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)
    
    
except:
    log_df_update(sqlContext,0,'Failed',get_pst_date(),'Parameters are improper','0',StartDate,' ',AlphaProcessDetailsLog_str)
    log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
    log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
    
    raise Exception("Parameters not present!!!")
    
if  job_type == 'prod':
    LaunchDate = launch_dt
else:
    LaunchDate = run_date

##### UDF'S for MIS module ##########
def BuildPath(env_type,job_type,test_type,LaunchDate,locale_name,cpgn_type):
    path_dict = {}
    for path in ['OcelotDataProcessing_Output','Mis_Output','TP_Output','User_Token_Output','RecipientID_Output',]:
        if  job_type != 'prod':
            if test_type in ('MANUAL','BACKUP','AUTOTEST'):
                path_dict[path] = "s3://occ-decisionengine/AlphaModularization/Environment_{}/Job_{}/Test_{}/{}/{}/{}/{}/".format(env_type,job_type,test_type,LaunchDate,locale_name,cpgn_type,path)
        else:
            path_dict[path] = "s3://occ-decisionengine/AlphaModularization/Environment_{}/Job_{}/{}/{}/{}/{}/".format(env_type,job_type,LaunchDate,locale_name,cpgn_type,path)
    return path_dict

path_dict = BuildPath(env_type,job_type,test_type,LaunchDate,locale_name,cpgn_type)

## function to extract joining key of MIS table
def extract_keys(var_source):
    temp1 = var_source.split("|")
    if len(temp1) > 1:
             keys = "##".join([val.split(".")[1] for val in temp1[1].split(";") if val!=None])
    else : keys = ""
    return keys  #columns that are used to join table

extract_keys_udf = udf(extract_keys,StringType())

### function to extract required cols from MIS table
def extract_cols(var_struct):
    import re
    content_cols = []
    if (var_struct.find('%%')>=0):
             z = [m.start() for m in re.finditer('%%', var_struct)]
             j = 0 
             while (j <= len(z)-2):
                    temp = var_struct[z[j]+2:z[j+1]]  #storing the data that lies between '%%'
                    temp_ls = temp.split('.')
                    if temp_ls[0] != 'traveler':
                         content_cols.append(temp_ls[1]) 

                    j+=2

    return "##".join(list(set(content_cols))) #columns that are required to extract data from (image,links,text etc.)

extract_cols_udf = udf(extract_cols,StringType())


### function to extract MIS table name
def file_name(var_source):
    join_key = var_source
    if( len(join_key.split('|')) > 1):
             return join_key.split('|')[1].split(';')[0].split('.')[0]
    
file_name_udf = udf(file_name,StringType())

def code_completion_email(body,subject,pos,locale_name):
    fromaddr = "expedia_notifications@affineanalytics.com"
    toaddr = ["alphaalerts@expedia.com"]
    toaddress = ", ".join(toaddr)
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddress
    msg['Subject'] = subject
    bodysub="\n [This is an automated mail triggered from one of your running application] \n \n Alpha process for " +str(pos)+" , "+str(locale_name)+ " (pos,locale)." + " has {}. \n \n Please do not reply directly to this e-mail. If you have any questions or comments regarding this email, please contact us at AlphaTechTeam@expedia.com."
    actual_message=bodysub.format(body) 
    msg.attach(MIMEText(actual_message, 'plain'))       
    server = smtplib.SMTP('smtp.office365.com', 587)
    server.starttls()
    server.login(fromaddr, "Affine@123")
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()
    print ("Code Completion mail triggered successfully")
    return "1"


#Need mis_data_rdd_dic
def readAllMisFiles():
    
    mapping_query = """(SELECT TableName as TableName,ServerName as ServerName from Alpha{}.dbo.DimContentTables with (NOLOCK)) foo """.format(env_type.title()) 
    mapping_table = sc.broadcast(importSQLTableForBusinessBasedSuppression(mapping_query, severName.value["mysql"]).toPandas().set_index("TableName").T.to_dict())
    
    for file_name in mis_data_file_names:
        
        try:
            if mapping_table.value[file_name]["ServerName"] == "MIS_LINK":
                file = importAURORATable("MIS",file_name)
            else:
                file = importSQLTable("AlphaMVP",file_name)
        except:
            log_df_update(sqlContext,0,'Failed',get_pst_date(),'MIS data is not available ' + file_name + " Alpha Ocelot contract violated Error Code : 1000",'0',StartDate,'',AlphaProcessDetailsLog_str)
            log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,'',AlphaProcessDetailsLog_str)
            log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,'',CentralLog_str)
            code_completion_email("failed due to MIS data not being present","Alpha process update for "+locale_name,pos,locale_name+file_name+" Alpha Ocelot contract violated Error Code : 1000")
            raise Exception("MIS data not present!!! "+file_name + " Alpha Ocelot contract violated Error Code : 1000")
            
        file = file.filter(pos_filter_cond)
        file.cache()
        mis_data_rdd_dic[file_name]=file
        #prints the schema, behaves as a pointer. doesn't store it in memory until this dictionary is used somewhere
        print("mis_data_rdd_dic -> ",mis_data_rdd_dic)
    return mis_data_rdd_dic

def readAllconfigloyaltyFiles():
    config_loyalty_data_rdd_dic={}
    for file_name in config_loyalty_file_name:
             file = importSQLTable(ocelotdb,file_name)
             file = file.filter(pos_filter_cond)
             file.cache()
             config_loyalty_data_rdd_dic[file_name]=file #prints the schema, behaves as a pointer. doesn't store it in memory until this dictionary is used somewhere
             print("config_loyalty_data_rdd_dic -> ",config_loyalty_data_rdd_dic)
    return config_loyalty_data_rdd_dic

def readOcelotOutput():

    test_flag = sqlContext.read.parquet(path_dict['OcelotDataProcessing_Output']+"TestFlag")
    module_version_test_flag = test_flag.select("flag").collect()[0][0]
    
    if(module_version_test_flag == '1'):
        dfMetaCampaignData_VarDef_backup = sqlContext.read.parquet(path_dict['OcelotDataProcessing_Output']+"dfMetaCampaignData_VarDef_backup")
    else:
        dfMetaCampaignData_VarDef_backup = sqlContext.read.parquet(path_dict['OcelotDataProcessing_Output']+"dfMetaCampaignData_VarDef")
        
    return dfMetaCampaignData_VarDef_backup
    
dfMetaCampaignData_VarDef_backup = readOcelotOutput()


def posFilters():
    pos_info = (dfMetaCampaignData_VarDef_backup.select("tpid","eapid").filter(" tpid is not null and eapid is not null").limit(1).rdd.first())
    pos_filter_cond = " tpid = " + str(pos_info["tpid"]) + " and eapid = " + str(pos_info["eapid"])
    posa_filter = "BRAND = 'EXPEDIA' and tpid = " + str(pos_info["tpid"]) + " and eapid = " + str(pos_info["eapid"])
    return pos_filter_cond,posa_filter
    
(pos_filter_cond,posa_filter) = posFilters()
    



def generateMisRdd():
    ### creating data frame having info about MIS tables
#Change 2: dfMetaCampaignData_VarDef_back up needs to be used as it has information about test published modules as well
    mis_data_df = (dfMetaCampaignData_VarDef_backup
                        .filter("var_source is not null")
                        .select("module_id","var_position","var_source","var_structure").distinct())
    mis_data_df.cache()
                        


### populating MIS table name , joining keys and cols required using above functions. Contains data that include the keys and columns required at a module id and var position level for each file
    mis_data_intermediate = (mis_data_df.withColumn("file_name",file_name_udf("var_source"))
                                     .withColumn("keys",extract_keys_udf("var_source"))
                                     .withColumn("mis_cols",extract_cols_udf("var_structure"))
                                     .filter("file_name is not null and mis_cols!='' and keys!='' ")
                                     .select("module_id","var_position","file_name","keys","mis_cols").distinct())
                                 

#mis_data_intermediate.show(100,False)  
### mis_data_intermediate is a table which contains file_name, keys, mis_cols
### Using the key in keys column the file_name will be joined to traveler data and the data from columns belonging to mis_cols will be fetched
    mis_data_file_names = mis_data_intermediate.select("file_name").distinct().rdd.map(lambda x: str(x[0])).collect() #finding out the file names
    config_loyalty_file_name = [x for x in mis_data_file_names if x=='config_loyalty' ]
    mis_data_file_names=[x for x in mis_data_file_names if x!='config_loyalty']
    mis_data_rdd =mis_data_intermediate.rdd.collect() #converting mis_data_intermediate to rdd
    return mis_data_file_names,config_loyalty_file_name,mis_data_rdd
    

(mis_data_file_names,config_loyalty_file_name,mis_data_rdd) = generateMisRdd()




def generateMisContentDict():
    
    mis_content_dict = {}
    mis_data_rdd_dic = {}
    config_loyalty_data_rdd_dic={}
    
    ### mis_data_rdd_dic is a dictionary with key as the filename and value as the schema. It acts as a pointer to that file
    # reading MIS data
    StartDate = get_pst_date()
    try:
        #print("mis_data_file_names,pos_filter_cond,mis_data_rdd_dic")
        #print(mis_data_file_names)
        #print(pos_filter_cond)
        #print(mis_data_rdd_dic)
        mis_data_rdd_dic = readAllMisFiles()
        config_loyalty_data_rdd_dic=readAllconfigloyaltyFiles()
        if(len(config_loyalty_file_name)!=0):
            mis_data_rdd_dic.update(config_loyalty_data_rdd_dic)
        print("Reading mis completed")
        log_df_update(sqlContext,1,'MIS data imported',get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)
    except:
        log_df_update(sqlContext,0,'Failed',get_pst_date(),'MIS data is not available','0',StartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
        code_completion_email("failed due to MIS data not being present","Alpha process update for "+locale_name,pos,locale_name)
        raise Exception("MIS data not present!!!")
        
    StartDate = get_pst_date()
    mis_content_dict = {}
    mis_not_present = []
    #goes along each row of mis_data_intermediate table(rdd version which is mis_data_rdd)
    for i in mis_data_rdd:
        try :
            module_id = i["module_id"]
            file_name = i["file_name"]
            join_keys = [val for val in i["keys"].split("##") if val!='']
            mis_cols = i["mis_cols"].split("##")
            cols_req = ["key"] + mis_cols             #format of [key, col1, col2....]
            content_df=(mis_data_rdd_dic[file_name].withColumn("locale",lower_locale_udf("locale")).filter(pos_filter_cond).withColumn("key",concat(*join_keys)).select(cols_req).toPandas())
            content_df.index = content_df.key
            #print("*********file_name= ",file_name)
            #print("---------Content_df--------")
            #content_df.show(5)
            content_dict = content_df[mis_cols].to_dict() #content_df- {col1: {value of join key: value of col1}, col2: {value of join key: value of col2}}
            mis_content_dict[str(module_id)+file_name+i["keys"].replace('##','')+str(i["var_position"])] = content_dict
        except:
            file_name = i["file_name"]
            module_id = i["module_id"]
            mis_not_present.append(file_name)
    print("mis_content_dict length -> ", len(mis_content_dict))
    print("mis_not_present lenght -> ",len(mis_not_present))        
    if len(mis_not_present) >0 :
        file_name_str = "#".join(mis_not_present) + " MIS files are not present"
        log_df_update(sqlContext,0,'Required MIS data not present',get_pst_date(),file_name_str,'0',StartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
        log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
        code_completion_email("failed due to MIS data not being present","Alpha process update for "+locale_name,pos,locale_name)
        raise Exception("MIS data not present!!!")
    else :
        log_df_update(sqlContext,1,'Required MIS data is present',get_pst_date()," ",'0',StartDate,' ',AlphaProcessDetailsLog_str)
        
    return mis_content_dict
    
mis_content_dict = generateMisContentDict()
    
StartDate = get_pst_date()
log_df_update(sqlContext,1,'MIS module finished',get_pst_date(),' ','0',StartDate,'',AlphaProcessDetailsLog_str)



def writeToS3(mis_content_dict):
    mis_dict_list = []
    mis_dict_list.append(mis_content_dict)
    schema = (StructType([StructField("Dictionary", MapType(StringType(),MapType(StringType(),MapType(StringType(),StringType()))), True)]))
    rdd_l = sc.parallelize(mis_dict_list)
    outputPath = path_dict['Mis_Output']+"misDictContent/mis_content_dict.txt"
    
    import boto
    
    conn = boto.connect_s3('AKIAJBGFKIM4C2OMBFFA', '5+FiqsQpPnaD36M7w5ij4+6fKZmn2/PNbnTpsGmY')
    bucket = conn.lookup('occ-decisionengine')
    if(job_type != 'prod'):
        if(test_type in ('MANUAL','BACKUP','AUTOTEST')):            
            prefix = "AlphaModularization/Environment_{}/Job_{}/Test_{}/{}/{}/{}/Mis_Output/".format(env_type,job_type,test_type,LaunchDate,locale_name,cpgn_type)
    else:
        prefix = "AlphaModularization/Environment_{}/Job_{}/{}/{}/{}/Mis_Output/".format(env_type,job_type,LaunchDate,locale_name,cpgn_type)
    print("Check If ouput folder already exist")
    for key in bucket.list(prefix):
        #If ouput folder already exists
        if key.name.find('misDictContent')!=-1:
            key.delete()
    print("Writing output")
    rdd_l.saveAsTextFile(outputPath)
    

writeToS3(mis_content_dict)

log_df_update(sqlContext,1,"MIS module has ended for {} Locale for {} LaunchDate".format(locale_name,LaunchDate),get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)