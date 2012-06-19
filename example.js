var couch = require('./couch.js');

var db = couch.init('testdb');


function next(callback) {
 return function(response) {
   if (response.error) {
     console.log('Error: ' + response.reason || response.error);
   }
   else callback(response);
  }
}

function start() {
  console.log('Creating db');
  db.createDb(next(insert));
}
     
function insert(response) {
  console.log('Saving doc');
  db.saveDoc({'name': 'Test document'}, next(retrieve));
}

function retrieve(response) {
  console.log('Retrieving doc ' + response.id);
  db.getDoc(response.id, next(remove));
}

function remove(doc) {
  console.log('Got doc with name: ' + doc.name);
  console.log('Removing doc ' + doc._id);
  db.deleteDoc(doc, next(stop));
}

function stop(response) {
  console.log('Deleting the db');
  db.deleteDb();
}

start();
