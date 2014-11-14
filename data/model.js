var mongoose = require('mongoose');

var wikiSchema = mongoose.Schema({
  title: { type: String, index: true},
  url: { type: String},
  abstract: { type: String},
  categories: {type: [{type: String}], index: true},
  links: {type: [{type: String}], index: true},
  headings: [{heading: {type: String}, position: {type: Number}}]},
  { collection: 'wiki' }
);

exports.WikiModel = mongoose.model('wiki', wikiSchema);