// A light-weight CouchDB nodejs client.

// couch.js (c) 2012 Jacob Sondergaard
// Licensed under the terms of the MIT license.

var http = require('http');
var qs = require('querystring');

exports.init = function(dbName, host, port) {

  host = host || '127.0.0.1';
  port = port || 5984;
  var basePath = '/' + dbName + '/';

  function simpleJSON(opts, callback) {
    opts.host = host;
    opts.port = port;
    opts.headers = {'Content-Type':'application/json'};

    var req = http.request(opts, function(res) {
      if (!callback) return;
      // buffer and parse JSON data and callback with object
      res.setEncoding('utf8');
      var data = '';      
      res.on('data', function(chunck) {
        data += chunck;
      });
      res.on('end', function() {
        callback(data ? JSON.parse(data) : {"error": "No data"});
      });
    });

    req.on('error', function(e) {
      if (callback) callback({"error":e.message});
    });

    req.end(JSON.stringify(opts.body));
  }
  
  function httpGet(path, callback) {
    var opts = {path:path, method:'GET'};
    simpleJSON(opts, callback);
  }

  function httpPost(path, body, callback) {
    var opts = {path:path, method:'POST', body:body};
    simpleJSON(opts, callback);
  }

  function httpPut(path, body, callback) {
    var opts = {path:path, method:'PUT', body:body};
    simpleJSON(opts, callback);
  }

  function httpDelete(path, callback) {
    var opts = {path:path, method:'DELETE'};
    simpleJSON(opts, callback);
  }

  function _view(path, params, callback) {
    var body;
    var options = {};
    for (var key in params) {
      if (key === 'keys') body = {'keys': params.keys};
      else options[key] = JSON.stringify(params[key]);
    }
    var query = qs.stringify(options);
    if (query) {
      path += '?' + query;
    }
    if (body) {
      httpPost(path, body, callback);
    }
    else {
      httpGet(path, callback);
    }
  }

  var couch = {

    //
    // Database operations
    //

    createDb: function(callback) {
      // Create database
      httpPut(basePath, undefined, callback);
    },

    deleteDb: function(callback) {
      // Delete database
      httpDelete(basePath, callback);
    },

    listDbs: function(callback) {
      // Lists databases on the server
      httpGet('/_all_dbs', callback);
    },

    infoDb: function(callback) {
      // Get info about the database
      httpGet(basePath, callback);
    },

    pullDb: function(source, callback, createTarget) {
      // Replicate changes from a source database to current (target) db
      var body = {source:source, target:dbName, create_target:createTarget};
      httpPost('/_replicate', body, callback);
    },

    uuids: function(count, callback) {
      count = count || 1;
      var path = '/_uuids';
      if (count > 1) {
        path += '?count=' + count.toString();
      }
      httpGet(path, function(res) {
        if (res.error) callback(res);
        else callback(res.uuids);
      });
    },

    //
    // Document operations
    //

    listDocs: function(callback) {
      // Get an object with id and rev of all documents in the database
      httpGet(basePath + '/_all_docs', function(res) {
        if (res.error) callback(res);
        else {
          var docs = {};
          for (var i in res.rows) {
            var row = res.rows[i];
            docs[row.id] = row.value.rev;
          }
          callback(docs);
        }
      });
    },

    getDoc: function(docId, callback) {
      // Get document with docId as document id
      httpGet(basePath + docId, callback);
    },

    getDocs: function(docIds, callback) {
      // Get multiple documents with document id's given in array docIds
      httpPost(basePath + '_all_docs?include_docs=true',
        {keys: docIds},
        function(res) {
          if (res.error) callback(res);
          else {
            var docs = [];
            for (var i in res.rows) {
              var row = res.rows[i];
              docs.push(row.doc || row.error);
            }
            callback(docs);
          }
      });
    },

    saveDoc: function(doc, callback) {
      // Save/create document. Call back with id and rev of document.
      if (doc._id && doc._rev) {
        httpPut(basePath + doc._id, doc, callback);
      }
      else {
        httpPost(basePath, doc, callback);
      }
    },

    saveDocs: function(docs, callback, allOrNothing) {
      // Save/create multiple documents. Call back with list containing
      // id and rev of the documents.
      var body = {docs:docs};
      if (allOrNothing) body['all_or_nothing'] = true;
      httpPost(basePath + '_bulk_docs', body, callback);
    },

    deleteDoc: function(doc, callback) {
      // Delete document
      var path = basePath + doc._id + '?' + qs.stringify({rev:doc._rev});
      httpDelete(path, callback);
    },

    deleteDocs: function(docs, callback, allOrNothing) {
      // Delete multiple documents
      for (var i in docs) {
        docs[i]._deleted = true;
      }
      var body = {docs:docs};
      if (allOrNothing) body['all_or_nothing'] = true;
      httpPost(basePath + '_bulk_docs', body, callback);
    },

    view: function(designDocName, viewName, params, callback) {
      //  Query a pre-defined view in the specified design doc.
      //  The following query parameters can be specified as the 'params' object.
      //
      //  Limit query results to those with the specified key or list of keys
      //    key=<key-value>
      //    keys=<list of keys>
      //    
      //  Limit query results to those following the specified startkey
      //    startkey=<key-value>
      //    
      //  First document id to include in the output
      //    startkey_docid=<document id>
      //    
      //  Limit query results to those previous to the specified endkey
      //    endkey=<key-value>
      //    
      //  Last document id to include in the output
      //    endkey_docid=<document id>
      //    
      //  Limit the number of documents in the output
      //    limit=<number of docs>
      //    
      //  If stale=ok is set CouchDB will not refresh the view even if it is stalled.
      //    stale=ok
      //    
      //  Reverse the output (default is false). Note that the descending option is
      //  applied before any key filtering, so you may need to swap the values of the
      //  startkey and endkey options to get the expected results.
      //    descending=true
      //    descending=false
      //    
      //  Skip the specified number of docs in the query results:
      //    skip=<number>
      //    
      //  The group option controls whether the reduce function reduces to a set of
      //  distinct keys or to a single result row:
      //    group=true
      //    group=false
      //  
      //    group_level=<number>
      //    
      //  Use the reduce function of the view. It defaults to true, if a reduce
      //  function is defined and to false otherwise.
      //    reduce=true
      //    reduce=false
      //  
      //  Automatically fetch and include the document which emitted each view
      //  entry (default is false).
      //    include_docs=true
      //    include_docs=false
      //  
      //  Controls whether the endkey is included in the result. It defaults to true.
      //    inclusive_end=true
      //    inclusive_end=false
      _view([basePath, '_design/', designDocName, '/_view/', viewName].join(''),
            params, callback);
    },

    viewAllDocs: function(params, callback) {
      // Query the _all_docs view.
      // Accepts same keyword parameters as view()
      _view(basePath + '_all_docs', params, callback);
    }

  };

  return couch;
};
