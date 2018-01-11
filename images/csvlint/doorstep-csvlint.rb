#!/usr/bin/env ruby
#
require 'csvlint'
require 'json'

output_file = ENV['LINTOL_OUTPUT_FILE']
metadata = ENV['LINTOL_METADATA']
input_data = ENV['LINTOL_INPUT_DATA']
data_file = ENV['LINTOL_DATA_FILE']

if !data_file
  data_files = Dir.entries(input_data)
  data_files.reject { | entry | File.directory?(entry) }
  data_file = data_files[0]
end

input_file = File.new(File.join(input_data, data_file))

validator = Csvlint::Validator.new(input_file)

translations = {
    :wrong_content_type => "Content type is not text/csv",
    :ragged_rows => "Row has a different number of columns (than the first row in the file)",
    :blank_rows => "Completely empty row, e.g. blank line or a line where all column values are empty",
    :invalid_encoding => "Encoding error when parsing row, e.g. because of invalid characters",
    :not_found => "HTTP 404 error when retrieving the data",
    :stray_quote => "Missing or stray quote",
    :unclosed_quote => "Unclosed quoted field",
    :whitespace => "A quoted column has leading or trailing whitespace",
    :line_breaks => "Line breaks were inconsistent or incorrectly specified"
}

report = []
if validator.errors
  report += [validator.errors.map { |error|
    [
      error.type,
      [
          translations[error.type],
          20,
          {
            "error-count" => 1,
            "valid" => false,
            "tables" => [
               {
                  "format" => "csv",
                  "errors" => [
                     {
                        "message" => "Row #{error.row}, #{error.column}: #{translations[error.type]}",
                        "row" => validator.data[error.row],
                        "row-number" => error.row,
                        "code" => "missing-value",
                        "column-number" => error.column
                     }
                  ],
                  "row-count" => 5,
                  "headers" => [
                     "ID",
                     "Name",
                     "N",
                     "Mean",
                     "Standard Deviation"
                  ],
                  "source" => data_file,
                  "time" => 0.016,
                  "valid" => false,
                  "scheme" => "file",
                  "encoding" => "utf-8",
                  "schema" => nil,
                  "error-count" => 1
               }
            ],
            "preset" => "table",
            "warnings" => [],
            "table-count" => 1,
            "time" => 0.02
         }

      ]
    ]
  }.to_h]
end

File.open(output_file, "w") do |file|
  file.puts JSON.pretty_generate(report)
end
