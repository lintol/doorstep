#!/usr/bin/env ruby
#
require 'csvlint'
require 'json'

output_file = ENV['LINTOL_OUTPUT_FILE']
metadata = ENV['LINTOL_METADATA']
input_data = ENV['LINTOL_INPUT_DATA']

data_files = Dir.entries(input_data)
data_files.reject { | entry | File.directory?(entry) }

input_file = File.join(input_data, data_files[0])

validator = Csvlint::Validator.new( File.new(input_file) )

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

report = [
  validator.errors.map { |error|
    [
      error.type,
      [
          translations[error.type],
          20,
          {
            :row => error.row,
            :column => error.column
          }
      ]
    ]
  }.to_h
]

File.open(output_file, "w") do |file|
  file.puts JSON.pretty_generate(report)
end
