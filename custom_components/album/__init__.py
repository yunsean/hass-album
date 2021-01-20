import json
import requests
import os,re,random,string
import hashlib
import time
import base64
import asyncio
import urllib
import requests
import re
import shlex
import async_timeout
import imp
import json
import datetime
import mimetypes
import hashlib

from aiohttp import web
from io import BytesIO
import logging
_LOGGER = logging.getLogger(__name__)

from aiohttp.web import Request, HTTPUnauthorized
import voluptuous as vol
import homeassistant.helpers.config_validation as cv
from homeassistant.components.http import (
    HomeAssistantView
)
from homeassistant.const import (
    ATTR_ENTITY_ID, HTTP_UNPROCESSABLE_ENTITY, HTTP_BAD_REQUEST, HTTP_SERVICE_UNAVAILABLE, HTTP_NOT_FOUND
)
from homeassistant.util import sanitize_path

CONF_STORAGE_PATH = 'storage_path'
ATTR_USER = 'user'
ATTR_LOCALPATH = 'path'
ATTR_MD5 = 'md5'
ATTR_OVERRIDE = 'override'
ATTR_TIME = "time"
ATTR_WANTTIME = "wanttime"
DOMAIN = 'album'

CONFIG_SCHEMA = vol.Schema({
    DOMAIN: vol.Schema({
        vol.Optional(CONF_STORAGE_PATH): cv.string,
        }),
    }, extra = vol.ALLOW_EXTRA)

def get_md5_01(file_path):
  md5 = None
  if os.path.isfile(file_path):
    f = open(file_path,'rb')
    md5_obj = hashlib.md5()
    md5_obj.update(f.read())
    hash_code = md5_obj.hexdigest()
    f.close()
    md5 = str(hash_code).lower()
  return md5

class AlumbSync(HomeAssistantView):
    """View to handle Geofency requests."""
    url = '/api/alumb/sync'
    name = 'api:alumb:sync'
    def __init__(self, storagePath):
        """Initialize Geofency url endpoints."""
        self._storagePath = storagePath
    @asyncio.coroutine
    def get(self, request):        
        queries = request.query
        if ATTR_USER not in queries :
            return ('User not specified.', HTTP_BAD_REQUEST)
        if ATTR_LOCALPATH not in queries :
            return ('Local path not specified.', HTTP_BAD_REQUEST)
        user = queries[ATTR_USER]
        if user != sanitize_path(user):
            raise web.HTTPBadRequest
        localpath = queries[ATTR_LOCALPATH].lstrip('/')
        if localpath != sanitize_path(localpath):
            raise web.HTTPBadRequest
        filepath = os.path.join(self._storagePath, user, localpath)
        if os.path.exists(filepath):
            md5 = get_md5_01(filepath)
            mtime = os.path.getmtime(filepath)
            return self.json({"exist": True, "md5": md5, "mtime": int(mtime)})
        return self.json({"exist": False})
    @asyncio.coroutine
    def delete(self, request):    
        _LOGGER.error("delete")    
        queries = request.query
        if ATTR_USER not in queries :
            return ('User not specified.', HTTP_BAD_REQUEST)
        if ATTR_LOCALPATH not in queries :
            return ('Local path not specified.', HTTP_BAD_REQUEST)
        user = queries[ATTR_USER]
        if user != sanitize_path(user):
            raise web.HTTPBadRequest
        localpath = queries[ATTR_LOCALPATH].lstrip('/')
        if localpath != sanitize_path(localpath):
            raise web.HTTPBadRequest
        filepath = os.path.join(self._storagePath, user, localpath)
        (parentpath,_) = os.path.split(filepath)
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except:
                return self.json({"result": "ng", "message": "删除文件失败！"})
        return self.json({"result": "ok"})
    @asyncio.coroutine
    def post(self, request):
        """Handle Geofency requests."""
        queries = request.query
        if ATTR_USER not in queries :
            return ('User not specified.', HTTP_BAD_REQUEST)
        if ATTR_LOCALPATH not in queries :
            return ('Local path not specified.', HTTP_BAD_REQUEST)
        user = queries[ATTR_USER]
        if user != sanitize_path(user):
            raise web.HTTPBadRequest
        localpath = queries[ATTR_LOCALPATH].lstrip('/')
        if localpath != sanitize_path(localpath):
            raise web.HTTPBadRequest
        filepath = os.path.join(self._storagePath, user, localpath)
        (parentpath,_) = os.path.split(filepath)
        os.makedirs(parentpath, exist_ok = True)
        message = None
        if os.path.exists(filepath):
            md5 = get_md5_01(filepath)
            if ATTR_MD5 in queries:
                if queries[ATTR_MD5] == md5:
                    return self.json({"result": "ok"})
            if ATTR_OVERRIDE not in queries:
                _LOGGER.warn(filepath + " is exist, md5 is " + md5)
                fsize = os.path.getsize(filepath)
                mtime = os.path.getmtime(filepath)
                return self.json({"result": "exist", "md5": md5, "mtime": int(mtime), "size": fsize})
            os.remove(filepath)
            message = "overrided"
        tempfile = filepath + ".partial"
        try:
            if os.path.exists(tempfile):
                os.remove(tempfile)
            with open(tempfile, 'wb') as fil:
                while True:
                    data = yield from request.content.read(1024000)
                    if not data:
                        break
                    fil.write(data)
            fil.close()
        except:
            _LOGGER.error("Can not create temp file: " + tempfile)
            return ('Can not create temp file.', HTTP_SERVICE_UNAVAILABLE)
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
            os.rename(tempfile, filepath)   
            if ATTR_TIME in queries:
                time = int(str(queries[ATTR_TIME]))
                os.utime(filepath, (time, time))     
            if message:
                return self.json({"result": "ok", "message": message})        
            else:
                return self.json({"result": "ok"})
        except:
            _LOGGER.error("Can not create target file: " + filepath)
            return ('Can not create target file.', HTTP_SERVICE_UNAVAILABLE)


class AlumbList(HomeAssistantView):
    """View to handle GPSLogger requests."""
    url = '/api/alumb/list'
    name = 'api:alumb:list'
    def __init__(self, storagePath):
        """Initialize Geofency url endpoints."""
        self._storagePath = storagePath
    async def get(self, request: Request):
        """Handle for GPSLogger message received as GET."""
        hass = request.app['hass']
        queries = request.query    
        if ATTR_USER not in queries :
            return ('User not specified.', HTTP_BAD_REQUEST)
        if ATTR_LOCALPATH in queries :
            localpath = queries[ATTR_LOCALPATH].lstrip('/')
        else:
            localpath = ""
        user = queries[ATTR_USER]
        if user != sanitize_path(user):
            raise web.HTTPBadRequest
        if localpath != sanitize_path(localpath):
            raise web.HTTPBadRequest
        wanttime = ATTR_WANTTIME in queries
        rootdir = os.path.join(self._storagePath, user, localpath)
        files = []
        folders = []
        if "pageIndex" in queries:
            pageIndex = int(queries["pageIndex"])
            pageSize = 20000 if ("pageSize" not in queries) else int(queries["pageSize"])
            if os.path.exists(rootdir) and os.path.isdir(rootdir):
                if pageIndex == 0:
                    for file in os.scandir(rootdir):
                        if not file.is_dir(follow_symlinks = False):
                            continue
                        line = file.name
                        if line.endswith('.partial'):
                            continue
                        if line.startswith("."):
                            continue
                        if line == '@eaDir':
                            continue
                        folders.append({"name": line})
                count = 0
                for file in os.scandir(rootdir):
                    if not file.is_file(follow_symlinks = False):
                        continue
                    line = file.name
                    if line.endswith('.partial'):
                        continue
                    if line.startswith("."):
                        continue
                    if line == '@eaDir':
                        continue
                    if count < pageSize * pageIndex:
                        count = count + 1
                        continue
                    count = count + 1
                    if count > pageSize * pageIndex + pageSize:
                        break
                    if wanttime:
                        stat = file.stat()
                        files.append({"name": line, "size": stat.st_size, "mtime": int(stat.st_mtime)})
                    else:
                        files.append({"name": line})
        else:
            if os.path.exists(rootdir) and os.path.isdir(rootdir):
                lines = os.scandir(rootdir)
                for file in lines:
                    line = file.name
                    if line.endswith('.partial'):
                        continue
                    if line.startswith("."):
                    	continue
                    if line == '@eaDir':
                    	continue
                    if wanttime:
                        stat = file.stat()
                        if file.is_dir(follow_symlinks = False):
                            folders.append({"name": line, "size": stat.st_size, "mtime": int(stat.st_mtime)})
                        if file.is_file(follow_symlinks = False):
                            files.append({"name": line, "size": stat.st_size, "mtime": int(stat.st_mtime)})
                    else:
                        if file.is_dir(follow_symlinks = False):
                            folders.append({"name": line})
                        if file.is_file(follow_symlinks = False):
                            files.append({"name": line})
        return self.json({"folders": folders, "files": files})


class AlumbPreview(HomeAssistantView):
    """View to handle GPSLogger requests."""
    url = '/api/alumb/preview'
    name = 'api:alumb:preview'
    def __init__(self, storagePath):
        """Initialize Geofency url endpoints."""
        self._storagePath = storagePath
    async def send(self, filepath, request, hasRange, start, end):
        resp = web.StreamResponse()
        headers = request.headers

        stat = os.stat(filepath)
        resp.content_length = stat.st_size
        resp.last_modified = stat.st_mtime

        totalLen = stat.st_size
        if hasRange and start is None and end is None:
            resp.set_status(200)
            hasRange = False
            resp.content_length = stat.st_size
            resp.headers['Accept-Ranges'] = 'none'
        elif hasRange:
            resp.set_status(206, 'Partial Content')
            if start is None:
                start = stat.st_size - end - 1
                end = stat.st_size - 1
            if end is None:
                end = stat.st_size - 1
            resp.headers["Content-Range"] = "bytes " + str(start) + "-" + str(end) + "/" + str(stat.st_size)
            _LOGGER.info("bytes " + str(start) + "-" + str(end) + "/" + str(stat.st_size))
            end = end + 1
            totalLen = end - start
            resp.content_length = totalLen
        else:
            resp.set_status(200)
            resp.content_length = stat.st_size

        resp.headers['Cache-Control'] = 'max-age=31536000'
        etag = hashlib.sha1(filepath.encode('utf-8')).hexdigest()
        resp.headers['ETag'] = etag
        mime = mimetypes.guess_type(filepath)
        if mime:
            mime = mime[0]
        else:
            mime = "application/octet-stream"
        resp.headers['Content-Type'] = mime
        resp.headers['Modify-Time'] = str(int(stat.st_mtime))

        with open(filepath, 'rb') as f:
            if hasRange:
                f.seek(start)
                await resp.prepare(request)
                while True:
                    len = min(10240, end - f.tell())
                    data = f.read(len)
                    if not data:
                        break
                    await resp.write(data)
                    await resp.drain()
            else:
                await resp.prepare(request)
                while True:
                    data = f.read(10240)
                    if not data:
                        break
                    await resp.write(data)
                    await resp.drain()
            await resp.write_eof()
            resp.force_close()
            return resp       

    async def get(self, request: Request):
        """Handle for GPSLogger message received as GET."""
        hass = request.app['hass']
        queries = request.query 
        headers = request.headers
        preview = False
        if ATTR_USER not in queries :
            return ('User not specified.', HTTP_BAD_REQUEST)
        if ATTR_LOCALPATH not in queries :
            return ('Local path not specified.', HTTP_BAD_REQUEST)
        user = queries[ATTR_USER]
        if user != sanitize_path(user):
            raise web.HTTPBadRequest
        localpath = queries[ATTR_LOCALPATH].lstrip('/')
        if localpath != sanitize_path(localpath):
            raise web.HTTPBadRequest
        rawfile = os.path.join(self._storagePath, user, localpath)
        parentpath = os.path.dirname(rawfile)
        thumbdir = os.path.join(parentpath, "@eaDir", os.path.basename(localpath))
        if os.path.exists(thumbdir):
            filepath = os.path.join(thumbdir, 'SYNOPHOTO_THUMB_L.jpg')
            if not os.path.exists(filepath):
                filepath = os.path.join(thumbdir, 'SYNOPHOTO_THUMB_XL.jpg')
                if not os.path.exists(filepath):
                    filepath = os.path.join(thumbdir, 'SYNOPHOTO_THUMB_M.jpg')
                    if not os.path.exists(filepath):
                        filepath = os.path.join(thumbdir, 'SYNOPHOTO_THUMB_SM.jpg')
        if not os.path.exists(filepath):
            filepath = rawfile
        if not os.path.exists(filepath):
            return ('File not exist.', HTTP_NOT_FOUND)     
        start = None
        end = None   
        hasRange = False
        try:
            if 'Range' in headers:    
                hasRange = True
                range = headers['Range']
                range = range.replace('bytes=', '')
                parts = range.split('-')      
                if parts[0].strip() != '':
                    start = int(str(parts[0].strip()))
                if parts[1].strip() != '':
                    end = int(str(parts[1].strip()))
        except:
            start = None
            end = Noe       
        resp = await self.send(filepath, request, hasRange, start, end)
        return resp

class AlumbDownload(HomeAssistantView):
    """View to handle GPSLogger requests."""
    url = '/api/alumb/download'
    name = 'api:alumb:download'
    def __init__(self, storagePath):
        """Initialize Geofency url endpoints."""
        self._storagePath = storagePath
    async def send(self, filepath, request, hasRange, start, end):
        resp = web.StreamResponse()
        headers = request.headers

        stat = os.stat(filepath)
        resp.content_length = stat.st_size
        resp.last_modified = stat.st_mtime

        totalLen = stat.st_size
        if hasRange and start is None and end is None:
            resp.set_status(200)
            hasRange = False
            resp.content_length = stat.st_size
            resp.headers['Accept-Ranges'] = 'none'
        elif hasRange:
            resp.set_status(206, 'Partial Content')
            if start is None:
                start = stat.st_size - end - 1
                end = stat.st_size - 1
            if end is None:
                end = stat.st_size - 1
            resp.headers["Content-Range"] = "bytes " + str(start) + "-" + str(end) + "/" + str(stat.st_size)
            _LOGGER.info("bytes " + str(start) + "-" + str(end) + "/" + str(stat.st_size))
            end = end + 1
            totalLen = end - start
            resp.content_length = totalLen
        else:
            resp.set_status(200)
            resp.content_length = stat.st_size

        resp.headers['Cache-Control'] = 'max-age=31536000'
        etag = hashlib.sha1(filepath.encode('utf-8')).hexdigest()
        resp.headers['ETag'] = etag
        mime = mimetypes.guess_type(filepath)
        if mime:
            mime = mime[0]
        else:
            mime = "application/octet-stream"
        resp.headers['Content-Type'] = mime
        resp.headers['Modify-Time'] = str(int(stat.st_mtime))

        with open(filepath, 'rb') as f:
            if hasRange:
                f.seek(start)
                await resp.prepare(request)
                while True:
                    len = min(10240, end - f.tell())
                    data = f.read(len)
                    if not data:
                        break
                    await resp.write(data)
                    await resp.drain()
            else:
                await resp.prepare(request)
                while True:
                    data = f.read(10240)
                    if not data:
                        break
                    await resp.write(data)
                    await resp.drain()
            await resp.write_eof()
            resp.force_close()
            return resp            

    async def get(self, request: Request):
        """Handle for GPSLogger message received as GET."""
        hass = request.app['hass']
        queries = request.query 
        headers = request.headers
        preview = False
        if ATTR_USER not in queries :
            return ('User not specified.', HTTP_BAD_REQUEST)
        if ATTR_LOCALPATH not in queries :
            return ('Local path not specified.', HTTP_BAD_REQUEST)
        user = queries[ATTR_USER]
        if user != sanitize_path(user):
            raise web.HTTPBadRequest
        localpath = queries[ATTR_LOCALPATH].lstrip('/')
        if localpath != sanitize_path(localpath):
            raise web.HTTPBadRequest
        filepath = os.path.join(self._storagePath, user, localpath)
        if not os.path.exists(filepath):
            return ('File not exist.', HTTP_NOT_FOUND)     
        start = None
        end = None   
        hasRange = False
        try:
            if 'Range' in headers:    
                hasRange = True
                range = headers['Range']
                range = range.replace('bytes=', '')
                parts = range.split('-')      
                if parts[0].strip() != '':
                    start = int(str(parts[0].strip()))
                if parts[1].strip() != '':
                    end = int(str(parts[1].strip()))
        except:
            start = None
            end = Noe       
        resp = await self.send(filepath, request, hasRange, start, end)
        return resp
    

class AlumbCheck(HomeAssistantView):
    """View to handle GPSLogger requests."""
    url = '/api/alumb/check'
    name = 'api:alumb:check'
    def __init__(self, storagePath):
        """Initialize Geofency url endpoints."""
        self._storagePath = storagePath       

    async def put(self, request: Request):
        """Handle for GPSLogger message received as GET."""
        hass = request.app['hass']
        try:
            data = await request.json()
        except ValueError:
            return self.json_message("Invalid JSON specified", HTTP_BAD_REQUEST)  
        queries = request.query
        if ATTR_USER not in queries :
            return ('User not specified.', HTTP_BAD_REQUEST)
        user = queries[ATTR_USER]
        if user != sanitize_path(user):
            raise web.HTTPBadRequest
        exists = []
        for item in data:
            path = item['path']
            filepath = os.path.join(self._storagePath, user, path.lstrip('/'))
            md5 = item['md5'] if 'md5' in item else None
            if os.path.exists(filepath) and os.path.isfile(filepath) and (not md5 or (get_md5_01(filepath) == md5)):
                exists.append(path)
        return self.json(exists)    

@asyncio.coroutine
def async_setup(hass, config):
    conf = config.get(DOMAIN, {})
    storagePath = conf.get(CONF_STORAGE_PATH)
    hass.http.register_view(AlumbSync(storagePath))
    hass.http.register_view(AlumbList(storagePath))
    hass.http.register_view(AlumbDownload(storagePath))
    hass.http.register_view(AlumbPreview(storagePath))
    hass.http.register_view(AlumbCheck(storagePath))
    return True
