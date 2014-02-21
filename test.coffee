liveDbPouch = require './index'

create = (callback) ->
  callback liveDbPouch()


describe 'pouch', ->
  require('livedb/test/snapshotdb') create
  require('livedb/test/oplog') create
