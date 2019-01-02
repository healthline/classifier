class ClassifierResult {
  constructor() {
    this.distance = 0.0;
    this.prototype_title = '';
    this.prototype_id = '';
  }

  toString() {
    return "{'prototype_id':" + this.prototype_id + ", 'prototype_title':" + this.prototype_title + ", 'distance':" + this.distance + "}";
  }
};

module.exports = ClassifierResult;
