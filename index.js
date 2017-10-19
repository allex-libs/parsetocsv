function createLib (execlib) {
  'use strict';

  return execlib.loadDependencies('client', ['allex:directory:lib', 'allex:httpresponsefile:lib'], require('./creator').bind(null, execlib));
}

module.exports = createLib;
