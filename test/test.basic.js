
describe ('Test Basic', function () {
  it('load Lib', function () {
    return setGlobal('Lib', require('../index')(execlib));
  });
  it('run Task', function (done) {
    taskRegistry.run('parse2csv', {
      infilepath: [__dirname, 'test.dir', 'FMH7035S_SRD1971_20170809.txt'],
      outfilepath: [__dirname, 'test.dir', 'FMH7035S_SRD1971_20170809.csv'],
      parsermodulename: 'allex__indata_northerntrust1971posparser',
      datamodulename: 'allex__indata_uploaddirectorypositioncontentsservice',
      cb: done.bind(null, undefined),
      includeheaders: true
    });
  });
});
