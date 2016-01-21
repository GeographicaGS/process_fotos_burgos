 # -*- coding:utf-8 -*-

import psycopg2
import os,sys,shutil
#from exceptions import Exception
from exceptions import Exception
import traceback
import multiprocessing
import math

if len(sys.argv)!= 3:
    cmd = "Bad parameters\n%s <source_folder> <destination_folder>" % (sys.argv[0])
    print cmd
    sys.exit(-1)

src = sys.argv[1]
dest = sys.argv[2]
# check if source folder exists
if not os.path.isdir(src):
    print "Source folder doesn't exist"
    sys.exit(-1)
# check if destination folder exists    
if not os.path.isdir(dest):
    print "Destination folder doesn't exist"
    sys.exit(-1)

# Database connection params    
HOST = "192.168.99.100"
DBNAME = "eiel_burgos"
PORT = 5432
PASSWORD = "eiel_burgos_admin"
USER = "eiel_burgos_admin"   
FASE = 2015

EDIFICIO_VGI_WIDTH = 258
EDIFICIO_VGI_HEIGHT = 189

MUNICIPIO_VGI_WIDTH = 258
MUNICIPIO_VGI_HEIGHT = 189

#Total number of pixels
PHOTO_TAB_N_PIXELES =   800 * 800

# Connect to database
conn = psycopg2.connect(host=HOST,dbname=DBNAME,port=PORT,user=USER,password=PASSWORD)
if conn == None:
    print "No connected"
    sys.exit(-1)

print "Launching"
cur = conn.cursor()
# clean target directory
os.system("rm -R " + dest + "/*")
# truncate table
cur.execute("TRUNCATE TABLE media.media;")

conn.commit()
cur.close()
conn.close()

import logging
logging.basicConfig(format='%(asctime)s %(message)s',filename='burgos_fotos.log',level=logging.DEBUG)

def process_towns(towns):
    
    conn = psycopg2.connect(host=HOST,dbname=DBNAME,port=PORT,user=USER,password=PASSWORD)
    if conn == None:
        log.error("Cannot connect to db")
        return

    cur = conn.cursor()

    # do the process for each folder, echa folder must be the INE code of the town
    for dir in towns:
        
        if dir[0] == ".":
            continue
            
        sys.stdout.write("Processing town " + dir)   
        sys.stdout.flush()

        # create the target folder
        os.mkdir(os.path.join(dest, dir))
        
        # process edificio VGI
        os.mkdir(os.path.join(dest, dir,"edificio_vgi"))
        sys.stdout.write(".")
       
        for edificio in os.listdir(os.path.join(src,dir,"edificio")):
            try:
                if edificio[0] ==".":
                    continue
                # rescaling the image
                fsrc = os.path.join(src,dir,"edificio",edificio)
                fdest= os.path.join(dest,dir,"edificio_vgi",edificio)
                cmd = "convert %s    -resize %dx%d^ %s" % (fsrc,EDIFICIO_VGI_WIDTH,EDIFICIO_VGI_HEIGHT,fdest)
                os.system(cmd)
                # insert into database
                fileName, fileExtension = os.path.splitext(edificio)
                cur.execute("INSERT INTO media.media (title,description,id_media_usage,id_media_type,id_equipment,path,id_object_type,visual_order,has_thumbnail) \
                            VALUES (%s, %s,%s,%s,%s,%s,null,-1,false)",(fileName,"",5,1,fileName,os.path.join(dir,"edificio_vgi",edificio)))
            except Exception as e:
                tb = traceback.format_exc()
                print e
                print tb
                logging.error("[%s] - Exception at edificio [%s]: %s\n%s\n\n" % (dir,edificio, e,tb))
                continue
            
        sys.stdout.write(".")
        sys.stdout.flush()
        
        # process photo tab    
        os.mkdir(os.path.join(dest, dir,"visor_fotos"))
        sys.stdout.write(".")
        sys.stdout.flush()
        
        for edificio in os.listdir(os.path.join(src,dir,"edificio")):
            try:
                if edificio[0] ==".":
                    continue;
                # rescaling the image
                fsrc = os.path.join(src,dir,"edificio",edificio)
                fdest= os.path.join(dest,dir,"visor_fotos",edificio)
                cmd = "convert %s    -resize %d@ %s" % (fsrc,PHOTO_TAB_N_PIXELES,fdest)        
                os.system(cmd)
                #get title and description
                fileName, fileExtension = os.path.splitext(edificio)
                
                sql = "SELECT * FROM mirador.servicies_structure WHERE service_identifier_column='%s'" % (fileName[:2])

                cur.execute(sql)
                result = cur.fetchone()
                if not result or not result[0]:
                    logging.error("Not found table to serach %s\n" %(edificio))
                    continue
                
                sql = "SELECT nombre FROM %s.%s WHERE %s='%s'" % (result[2],result[3],result[4].replace('|','||'),str(FASE)+fileName)       
                cur.execute(sql)        
                result = cur.fetchone()
                
                if not result or not result[0]:
                    logging.error("Not found element %s [%s]\n" %(edificio,sql))
                    continue
                
            
                #insert into database
                cur.execute("INSERT INTO media.media (title,description,id_media_usage,id_media_type,id_equipment,path,id_object_type,visual_order,has_thumbnail) \
                            VALUES (%s, %s,%s,%s,%s,%s,null,-1,false)",(result[0],"",1,1,fileName,os.path.join(dir,"visor_fotos",edificio)))
            except Exception as e:
                tb = traceback.format_exc()
                print e
                print tb
                logging.error("[%s] - Exception at edificio [%s][visor_fotos]: %s\n%s\n\n" % (dir,edificio, e,tb))
                continue

            
        try:      
            # process municipio vgi 
            os.mkdir(os.path.join(dest, dir,"municipio_vgi"))
            sys.stdout.write(".")
            sys.stdout.flush()
            
            fsrc = os.path.join(src,dir,"municipio",dir + ".jpg")
            fdest = os.path.join(dest,dir,"municipio_vgi",dir + ".jpg")
            cmd = "convert %s    -resize %dx%d^ %s" % (fsrc,MUNICIPIO_VGI_WIDTH,MUNICIPIO_VGI_HEIGHT,fdest)
            os.system(cmd)
            
            cur.execute("INSERT INTO media.media (title,description,id_media_usage,id_media_type,id_equipment,path,id_object_type,visual_order,has_thumbnail) \
                            VALUES (%s, %s,%s,%s,%s,%s,null,-1,false)",(dir,"",3,1,dir,os.path.join(dir,"municipio_vgi",dir + ".jpg")))

        except Exception as e:
            tb = traceback.format_exc()
            print e
            print tb
            logging.error("[%s] Exception municipio_vgi:%s\n%s\n\n" % (dir, e,tb))
            

        # process nucleo vgi
        os.mkdir(os.path.join(dest, dir,"nucleo_vgi"))
        sys.stdout.write(".")
        sys.stdout.flush()
        
        for nucleo in os.listdir(os.path.join(src,dir,"nucleo")):
            try:
                if nucleo[0] ==".":
                    continue;
                # rescaling the image
                fsrc = os.path.join(src,dir,"nucleo",nucleo)
                fdest= os.path.join(dest,dir,"nucleo_vgi",nucleo)
                cmd = "convert %s    -resize %dx%d^ %s" % (fsrc,MUNICIPIO_VGI_WIDTH,MUNICIPIO_VGI_HEIGHT,fdest)
                os.system(cmd)
            
                #insert into database
                fileName, fileExtension = os.path.splitext(nucleo)
                cur.execute("INSERT INTO media.media (title,description,id_media_usage,id_media_type,id_equipment,path,id_object_type,visual_order,has_thumbnail) \
                            VALUES (%s, %s,%s,%s,%s,%s,null,-1,false)",(fileName,"",6,1,fileName,os.path.join(dir,"nucleo_vgi",nucleo)))
                
            except Exception as e:
                tb = traceback.format_exc()
                print e
                print tb
                logging.error("[%s] Exception nucleo_vgi[%s] :%s\n%s\n\n" % (dir,nucleo, e,tb))
                continue

        try:
            # process municipio principal pestanya de fotos 
            os.mkdir(os.path.join(dest, dir,"municipio_principal_pest_fotos"))
            sys.stdout.write(".")
            sys.stdout.flush()
            
            #copy photo no rescaling need
            fsrc = os.path.join(src,dir,"municipio_ppal_pestanya_fotos",dir + ".jpg")
            fdest = os.path.join(dest,dir,"municipio_principal_pest_fotos",dir + ".jpg")
            shutil.copyfile(fsrc,fdest)
            
            #insert into database
            cur.execute("INSERT INTO media.media (title,description,id_media_usage,id_media_type,id_equipment,path,id_object_type,visual_order,has_thumbnail) \
                            VALUES (%s, %s,%s,%s,%s,%s,null,-1,false)",(dir,"",4,1,dir,os.path.join(dir,"municipio_principal_pest_fotos",dir + ".jpg")))

        except Exception as e:
            tb = traceback.format_exc()
            print e
            print tb
            logging.error("[%s] Exception pestaña foto: %s\n%s\n\n" % (dir, e,tb))
                

    conn.commit()
    cur.close()
    conn.close()

def bucketize(towns,blocks):
    size = len(towns)/blocks
    return zip(*[iter(towns)]*size)

towns = os.listdir(src)

towns.remove('._.DS_Store')
towns.remove('.DS_Store')
ncpus = multiprocessing.cpu_count()

buckets = bucketize(towns,ncpus)

process = []
total = []

for b in buckets:
    total.extend(b)
    p = multiprocessing.Process(target=process_towns, args=(b,))
    p.start()
    process.append(p)

# BEGIN trick FIX this at bucketize!! I think it should be a math.ceil at bucket size.
diff = list(set(towns) - set(total))
p = multiprocessing.Process(target=process_towns, args=(diff,))
p.start()
process.append(p)
# END trick

for p in process:
    p.join()
    



#