liveDbPouch = require './index'


clear = (callback) ->
  PouchDB = require 'pouchdb'
  PouchDB.destroy 'testcollection', ->
    PouchDB.destroy 'testcollection_ops', ->
      callback()


create = (callback) ->
  clear ->
    callback liveDbPouch()


describe 'pouch', ->
  afterEach (done) ->
    clear done

  require('livedb/test/snapshotdb') create
  require('livedb/test/oplog') create
