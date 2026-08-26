module.exports = sohara.flow("js-flow")
  .source("in", "inline", {
    records: [
      { name: "Alice", age: 30 },
      { name: "Bob", age: 15 }
    ]
  })
  .transform("adult", "filter", { where: "age >= 18" })
  .transform("tag", "add_field", { field: "ok", value: true })
  .sink("out", "file", {
    path: "./output/js-flow.jsonl",
    format: "jsonl"
  })
  .build();
