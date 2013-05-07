import psycopg2
import os,sys,shutil

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
HOST = "aurora"
DBNAME = "eiel_burgos_dev"
PORT = 5434
PASSWORD = "eiel_burgos"
USER = "eiel_burgos_admin"   

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

# do the process for each folder, echa folder must be the INE code of the town
for dir in os.listdir(src):
    
    if dir[0] == ".":
        continue
        
    sys.stdout.write("Processing town " + dir)    
    # create the target folder
    os.mkdir(os.path.join(dest, dir))
    
    # process edificio VGI
    os.mkdir(os.path.join(dest, dir,"edificio_vgi"))
    sys.stdout.write(".")
   
    for edificio in os.listdir(os.path.join(src,dir,"edificio")):
        continue
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
        
    sys.stdout.write(".")
    
    # process photo tab    
    os.mkdir(os.path.join(dest, dir,"visor_fotos"))
    sys.stdout.write(".")
    
    for edificio in os.listdir(os.path.join(src,dir,"edificio")):
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
            print "Bad code for " + edificio
            continue
        
        sql = "SELECT nombre FROM %s.%s WHERE %s='%s'" % (result[2],result[3],result[4].replace('|','||'),"2012"+fileName)       
        cur.execute(sql)        
        result = cur.fetchone()
        
        if not result or not result[0]:
            print "Not inserted "+ edificio
            continue
        
        #insert into database
        cur.execute("INSERT INTO media.media (title,description,id_media_usage,id_media_type,id_equipment,path,id_object_type,visual_order,has_thumbnail) \
                    VALUES (%s, %s,%s,%s,%s,%s,null,-1,false)",(result[0],"",1,1,fileName,os.path.join(dir,"visor_fotos",edificio)))
        
        
    # process municipio vgi 
    os.mkdir(os.path.join(dest, dir,"municipio_vgi"))
    sys.stdout.write(".")
    
    fsrc = os.path.join(src,dir,"municipio",dir + ".jpg")
    fdest = os.path.join(dest,dir,"municipio_vgi",dir + ".jpg")
    cmd = "convert %s    -resize %dx%d^ %s" % (fsrc,MUNICIPIO_VGI_WIDTH,MUNICIPIO_VGI_HEIGHT,fdest)
    os.system(cmd)
    
    cur.execute("INSERT INTO media.media (title,description,id_media_usage,id_media_type,id_equipment,path,id_object_type,visual_order,has_thumbnail) \
                    VALUES (%s, %s,%s,%s,%s,%s,null,-1,false)",(dir,"",3,1,dir,os.path.join(dir,"municipio_vgi",dir + ".jpg")))
    
    # process nucleo vgi
    os.mkdir(os.path.join(dest, dir,"nucleo_vgi"))
    sys.stdout.write(".")
    
    for nucleo in os.listdir(os.path.join(src,dir,"nucleo")):
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
                    VALUES (%s, %s,%s,%s,%s,%s,null,-1,false)",(fileName,"",6,1,fileName,os.path.join(dir,"nucleo_vgi",edificio)))
        
    # process municipio principal pestanya de fotos 
    os.mkdir(os.path.join(dest, dir,"municipio_principal_pest_fotos"))
    sys.stdout.write(".")
    
    #copy photo no rescaling need
    fsrc = os.path.join(src,dir,"municipio_ppal_pestanya_fotos",dir + ".jpg")
    fdest = os.path.join(dest,dir,"municipio_principal_pest_fotos",dir + ".jpg")
    shutil.copyfile(fsrc,fdest)
    
    #insert into database
    cur.execute("INSERT INTO media.media (title,description,id_media_usage,id_media_type,id_equipment,path,id_object_type,visual_order,has_thumbnail) \
                    VALUES (%s, %s,%s,%s,%s,%s,null,-1,false)",(dir,"",4,1,dir,os.path.join(dir,"municipio_principal_pest_fotos",dir + ".jpg")))
    

conn.commit()
cur.close()
conn.close()
