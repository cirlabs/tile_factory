#!/usr/bin/env python
from fabric.api import local, shell_env

"""
This fabfile will allow us to style, render and deploy our map tiles and utf grids to s3.
"""

# A list of map MBTiles files (don't include .mbtiles) to be used when calling deploy_all() Example: ['MyMapMBTiles1', 'MyMapMBTiles2']
MAPS_LIST = []

# Directories for retrieving/storing various files
PROJECT_DIRECTORY = '/absolute/path/to/where/MBTiles/files/are/found/and/where/export/will/go/'
S3_DIRECTORY_S3CMD = 's3://bucket/'
S3_DIRECTORY = 'website.org/public/url/of/s3/bucket/'
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
VERSION = '1.0.0'

DELETE_LIST = []  # List of map tile folders to delete, including version if present.
DELETE_BUCKET = 'your.bucket.name'
DELETE_BASE_DIRECTORY = 'file/path/inside/that/bucket'

# Generally, don't specify those variables above, use a separate file (not committed) called local_config.py
try:
    from local_config import *
except ImportError:
    pass


# Functions
def deploy_grids_only():
    """
    Deletes .png files, leaving only grid files. Need to make this work differently.
    """

    for map in MAPS_LIST:
        #copy_map_dirs(map)
        #extract_tiles(map)
        reap_pngs_spare_grids(map)
        deploy_map(map)


def deploy_all():
    """
    Copies the .mbtils file from exported MapBox projects, runs mb-util, deploys to s3
    Use this when all maps have changed.
    """

    for map in MAPS_LIST:
        copy_map_dirs(map)
        extract_tiles(map)
        deploy_map(map)
        deploy_json(map)


def delete_all():
    """
    Deletes everything in the DELETE_LIST. Use with extreme caution.
    """

    for map in DELETE_LIST:
        delete_tileset(map)


def deploy_json(map):
    """
    Sends json file to s3
    """
    command = 's3cmd put -P ' + PROJECT_DIRECTORY + map + '.json ' + S3_DIRECTORY_S3CMD + map + '/' + VERSION
    local(command)


def copy_map_dirs(map):
    """
    Sets up dirs for map, copies .mbtiles file
    """
    print 'Making map directory: ' + map
    command = 'mkdir -p ' + PROJECT_DIRECTORY + 'tiles_' + map
    local(command)

    #print 'Copying .mbtiles file: ' + map
    command = 'cp ' + PROJECT_DIRECTORY + map + '.mbtiles ' + PROJECT_DIRECTORY + 'tiles_' + map + '/'
    local(command)


def extract_tiles(map):
    """
    Removes previously rendered tiles, then runs mb-util on the .mbtiles file.
    """
    print 'Removing previously rendered tyles for map: ' + map
    command = 'rm -Rf ' + PROJECT_DIRECTORY + 'tiles_' + map + '/rendered_tiles'
    local(command)

    print 'Running mb-util for map: ' + map
    #command = 'mb-util ' + map + '.mbtiles ' + PROJECT_DIRECTORY + ' ' + PROJECT_DIRECTORY + 'tiles_' + map + '/rendered_tiles'
    command = 'mb-util ' + PROJECT_DIRECTORY + 'tiles_' + map + '/' + map + '.mbtiles ' + PROJECT_DIRECTORY + 'rendered_tiles'
    local(command)

    print 'Moving json file into rendered_tiles'
    command = 'mv ' + PROJECT_DIRECTORY + map + '.json ' + 'rendered_tiles/'

    print 'Moving rendered_tiles into correct directory'
    command = 'mv ' + PROJECT_DIRECTORY + 'rendered_tiles ' + PROJECT_DIRECTORY + 'tiles_' + map + '/'
    local(command)

#     print 'Fixing version number'
#     command = 'mv ' + PROJECT_DIRECTORY + 'tiles_' + map + '/rendered_tiles/ ' + PROJECT_DIRECTORY + 'tiles_' + map + '/rendered_tiles/' + VERSION + '/'
#     local(command)


def reap_pngs_spare_grids(map):
    """
    Removes all the pngs, leaving only the grids, renaming grids to not overwrite original ones
    """
    print 'Removing pngs'
    #command = 'rm -rf ./' + PROJECT_DIRECTORY + 'tiles_' + map + '/'
    #command = 'find ./' + PROJECT_DIRECTORY + 'tiles_' + map + '/ -type f -name \'*.png\' | xargs rm'
    command = 'find ' + PROJECT_DIRECTORY + 'tiles_' + map + ' -type f -name \'*.png\' | xargs rm'
    local(command)


def extract_grids(rootpath, minzoom, maxzoom):
    """
    Grabs UTFGrid files from given zoom levels and puts them in a separate folder structure (matching original)
    """
    zoomrange = range(int(minzoom), int(maxzoom)+1)
    for z in zoomrange:
        command = 'find ' + rootpath + '/' + str(z) + '/ -name \'*.json\' -print | cpio -pvdumB ' + rootpath + '/extracted_grids_temp'
        local(command)
    #move whole shebang back out of full directory structure
    local('mv ' + rootpath + '/extracted_grids_temp' + rootpath + ' ' + rootpath + '/extracted_grids')
    local('rm -rf ' + rootpath + '/extracted_grids_temp')


def deploy_map(map):
    """
    Deploys the rendered tiles to s3.
    """

    with shell_env(AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY):

        print 'Deploying map: ' + map
        #command = 's3cmd put --recursive -P ' + PROJECT_DIRECTORY + 'tiles_' + map + '/rendered_tiles/ ' + S3_DIRECTORY_S3CMD
        command = 'ivs3 --concurrency 64 --acl-public ' + PROJECT_DIRECTORY + 'tiles_' + map + '/rendered_tiles/ ' + S3_DIRECTORY + map + '/' + VERSION
        local(command)


def delete_tileset(map):
    """
    Concurrently deletes giant tileset folders
    """

    with shell_env(AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY):

        print "Deleting map: " + map

        command = 'python ivs3_delete.py --concurrency 64 ' + DELETE_BUCKET + ' ' + DELETE_BASE_DIRECTORY + '/' + map + '/'
        local(command)
