function createTask (execlib, dirlib, filelib) {
  'use strict';

  var lib = execlib.lib,
    q = lib.q,
    execSuite = execlib.execSuite,
    taskRegistry = execSuite.taskRegistry,
    Task = execSuite.Task,
    Node = require('allex_nodehelpersserverruntimelib')(lib),
    Fs = Node.Fs,
    Path = Node.Path,
    FsUtils = require('allex_fsutilsserverruntimelib')(lib),
    CsvFile = filelib.CsvFile;

  function Parse2CsvTask (prophash) {
    Task.call(this, prophash);
    this.infilepath = prophash.infilepath;
    this.outfilepath = prophash.outfilepath;
    this.parsermodulename = prophash.parsermodulename;
    this.datamodulename = prophash.datamodulename;
    this.metamap = prophash.metamap;
    this.cb = prophash.cb;
    this.errorcb = prophash.errorcb;
    this.includeheaders = prophash.includeheaders;

    this.dataservicesink = null;
    this.datausersink = null;
    this.dirdb = null;
    this.recordcount = 0;
  }
  lib.inherit (Parse2CsvTask, Task);
  Parse2CsvTask.prototype.destroy = function () {
    this.recordcount = null;
    if (this.dirdb) {
      this.dirdb.destroy();
    }
    this.dirdb = null;
    if (this.datausersink) {
      this.datausersink.destroy();
    }
    this.datausersink = null;
    if (this.dataservicesink) {
      this.dataservicesink.destroy();
    }
    this.dataservicesink = null;
    this.cb = null;
    this.metamap = null;
    this.datamodulename = null;
    this.parsermodulename = null;
    this.outfilepath = null;
    this.infilepath = null;
    Task.prototype.destroy.call(this);
  };
  Parse2CsvTask.prototype.go = function () {
    execSuite.start({
      service: {
        modulename: this.datamodulename
      }
    }).then(this.onDataService.bind(this));
  };
  Parse2CsvTask.prototype.onDataService = function (servicesink) {
    if (!servicesink) {
      this.fail('COULD_NOT_CREATE_DATA_SERVICE', 'No Data service could be created from module '+this.datamodulename);
      return;
    }
    this.dataservicesink = servicesink;
    servicesink.subConnect('.', {name: 'user', role: 'user'}).then(
      this.onDataServiceUser.bind(this)
    );
  };
  Parse2CsvTask.prototype.onDataServiceUser = function (usersink) {
    if (!usersink) {
      this.fail('COULD_NOT_SUBCONNECT_AS_USER', 'No usersink obtained on '+this.datamodulename+' data service');
      return;
    }
    this.datausersink = usersink;
    this.goParse();
  };
  Parse2CsvTask.prototype.goParse = function () {
    var path = FsUtils.surePath(this.infilepath),
      dir = Path.dirname(path),
      d = q.defer(),
      path = FsUtils.surePath(this.infilepath);
    this.dirdb = new dirlib.DataBase(dir);
    d.promise.then(
      this.onFileReader.bind(this),
      this.onFileReaderFailed.bind(this),
      this.onRecord.bind(this)
    );
    this.dirdb.read(Path.basename(path), {parsermodulename: this.parsermodulename},d);
  };
  Parse2CsvTask.prototype.onFileReader = function (reader) {
    //need to wait a bit for all the records to be there
    lib.runNext(this.makeCsv.bind(this,reader), 100);
  };
  Parse2CsvTask.prototype.makeCsv = function (reader) {
    var csv = new CsvFile (this.outfilepath);
    if (this.includeheaders) {
      csv.includeHeaders = true;
    }
    csv.streamInFromDataSink(this.datausersink, {}).then(this.onCsvMade.bind(this));
  };
  Parse2CsvTask.prototype.onCsvMade = function (result) {
    if (this.outfilepath) {
      Fs.writeFile(FsUtils.surePath(this.outfilepath), result, this.onCsvWritten.bind(this));
      return;
    }
    this.success(result);
  };
  Parse2CsvTask.prototype.onCsvWritten = function (err) {
    if (err) {
      this.fail(err);
    } else {
      this.success(this.recordcount);
    }
  };
  Parse2CsvTask.prototype.onRecord = function (record) {
    if (!this.datausersink) {
      this.fail('DATA_USER_SINK_DEAD', 'User Sink to Data service turned out to be dead during parse');
      return;
    }
    this.recordcount ++;
    if (this.metamap) {
      lib.extend(record, this.metamap);
    }
    this.datausersink.call('create', record);
  };
  Parse2CsvTask.prototype.onFileReaderFailed = function (error) {
    this.fail(error.code, error.message);
  };
  Parse2CsvTask.prototype.success = function (result) {
    if (this.cb) {
      this.cb(result);
    }
    this.destroy();
  };
  Parse2CsvTask.prototype.fail = function (code, message) {
    if (this.errorcb) {
      this.errorcb(new lib.Error(code, message));
    }
    this.destroy();
  };
  Parse2CsvTask.prototype.compulsoryConstructionProperties = ['infilepath', 'parsermodulename', 'datamodulename'];


  taskRegistry.register('allex_parse2csvlib', [{name: 'parse2csv', klass: Parse2CsvTask}]);
}

module.exports = createTask;
