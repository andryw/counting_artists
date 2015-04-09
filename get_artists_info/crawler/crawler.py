__author__ = 'andryw'
import urlparse
import csv
import  urllib2,urllib, json

def save(name):
    args = {}

    args["api_key"] = "NNDIE5MEWU4J2ZPJQ"
    args["format"]="json"
    args["bucket"]="artist_location"
    args["name"]=name

    url=urlparse.urlunparse(('http',
        'developer.echonest.com',
        '/api/v4/artist/profile',
        '',
        urllib.urlencode(args),
    ''))

    req = urllib2.Request(url + "&bucket=genre" )
    f = urllib2.urlopen(req)
    a= json.loads(f.read())
    response = a["response"]
    artist = response['artist']
    with open("/home/andryw/Documents/artists/" + name + '.json', 'w') as outfile:
        json.dump(artist, outfile)
        print name


with open("/home/andryw/Documents/artists_coocurrence_5000/artists_count_5000users",'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=';')
    for row in spamreader:
        save(row[0])




