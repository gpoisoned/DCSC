import sys,os
import pickle
import ScanPlate
import GetLatLon
import tempfile
import PIL
import pika
import RedisHelper

def imageType(filename):
    try:
        i=PIL.Image.open(filename)
        return i.format
    except IOError:
        return False

hostname= os.environ['RABBIT_HOST'] if 'RABBIT_HOST' in os.environ else 'rabbitmq-server.local'

def photoInfo(pickled):
    #
    # You can print it out, but it is very long
    print "pickled item is ", len(pickled),"bytes"
    unpickled = pickle.loads(pickled)
    filename = unpickled[0]
    digest = unpickled[1]
    photo = unpickled[2]
    print "File name was", filename, "digest is ", digest
    photoFile,photoName = tempfile.mkstemp("photo")
    os.write(photoFile, photo)
    os.close(photoFile)
    newPhotoName = photoName + '.' + imageType(photoName)
    os.rename(photoName, newPhotoName)
    print "Wrote it to ", newPhotoName

    licenses = ScanPlate.getLikelyLicense( newPhotoName )
    print "License:", licences
    # If found likely licenses, send to redis db
    if licences:
        redisByChecksum = RedisHelper('redis-server.local', 1)
        if redisByChecksum.sendList(digest, licences):
            print "Sucessfully saved to Redis Database - 1"
        else:
            print "Error: Something is wrong with the list provided"

        redisByName = RedisHelper('redis-server.local', 2)
        if redisByName.sendList(filename, licenses):
            print "Sucessfully saved to Redis Database - 2"
        else:
            print "Error: Something is wrong with the list provided"

        redisMD5ByLicense = RedisHelper('redis-server.local', 3)
        redisNameByLicense = RedisHelper('redis-server.local', 4)
        for licence in licences:
            if redisMD5ByLicense.sendList(license, [digest]):
                print "Sucessfully saved to Redis Database - 3"
            else:
                print "Error: Something is wrong with the list provided"

            if redisNameByLicense.sendList(license, [filename]):
                print "Sucessfully saved to Redis Database - 3"
            else:
                print "Error: Something is wrong with the list provided"

    print "GeoTag:", GetLatLon.getLatLon( newPhotoName )
    os.remove(newPhotoName)

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=hostname))
channel = connection.channel()

channel.exchange_declare(exchange='scanners',type='fanout')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='scanners',queue=queue_name)

print ' [*] Waiting for logs. To exit press CTRL+C'

def callback(ch, method, properties, body):
    photoInfo(body)

channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()
