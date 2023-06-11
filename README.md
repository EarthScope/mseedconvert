# mseedconvert - miniSEED data format converter

A general purpose tool for converting between variations of miniSEED
formatted data.  For example, this converter can convert:
* format version 2 to 3 (or vise versa)
* convert data encodings
* change record lengths

While care is taken to preserve all characteristics of the original data
and most conversions are lossless, some combinations of data content and
options may result in loss of information.

## Downloading and building

The [releases](https://github.com/earthscope/mseedconvert/releases) area
contains release versions.

In most environments a simple 'make' will build the program.

The CC and CFLAGS environment variables can be used to configure
the build parameters.

## Usage

```shell
Usage: mseedconvert [options] -o outfile infile

 ## Options ##
 -V             Report program version
 -h             Show this usage message
 -v             Be more verbose, multiple flags can be used
 -f             Force full repack of encoded data, do not use shortcut
 -R bytes       Specify record length in bytes for packing
 -E encoding    Specify encoding format for packing
 -F version     Specify output format version, default is 3
 -eh JSONFile   Specify file with an extra header JSON Merge Patch

 -o outfile     Specify the output file, required

 infile         Input miniSEED file

Each record is converted independently.  This can lead to unfilled records
that contain padding depending on the conversion options.
```

When writing format 3, encoded data samples are copied verbatim when
no conversion is necessary.  This avoids the costly decoding and
re-encoding of data samples.  This functionality can be disabled using
the `-f` (force repack) option.

## Modifying Extra Headers during conversion

The `-eh` option specifies a file containing a JSON Merge Patch
[https://datatracker.ietf.org/doc/html/rfc7386](RFC 7386)
that is applied during conversion of each record.

A Merge Patch can be used to add, modify, or delete extra headers.

## Examples

#### Converting version 2 to 3

Converting miniSEED version 2 to 3 is lossless is virtually all but
the most escoteric cases.  In this case the value specified with `-R`
is the maximum record length to create, default maximum is 131,172 bytes.
Resulting record lengths will be exactly the size needed to contain the
data of each input record up to the maximum.

```shell
% mseedconvert data.mseed2 -o data.mseed3
```

Input data can also be read from standard input and written to standard
output, effectively functioning as a streaming converter.  Specify the
input and/or output files as `-` to utilize this functionality.

```shell
% cat data.mseed2 | mseedconvert - -o - > data.mseed3
```

#### Converting version 3 to 2

When converting format version 3 to 2, you will want to specify a
record length, which must be a power of 2.

```shell
% mseedconvert data.mseed3 -R 4096 -o data.mseed2
```

#### Converting data encodings

Converting a legacy encoding like SRO to Steim-1, while also converting
to miniSEED version 3:

```shell
% mseedconvert testdata-encoding-SRO.mseed2 -E 10 -o testdata-encoding-Steim1.mseed3
```

#### Modifying Extra Headers (or v2 Blockettes) during conversion

Any specified Merge Patch is applied to every converted data record.

A JSON Merge Patch that specifies custom, non-FDSN headers:

```shell
% cat extraheader_patch.json
{
  "OperatorXYZ": {
    "DSP": {
      "PeakRMS": 2067,
      "RMSWindow": 10.5
    }
  }
}
```

Applying this patch using the `-eh` option will replace these headers
if they exist and otherwise add them.  All other headers will remain
uneffected.

```shell
% mseedconvert data.mseed2 -o data.mseed3 -eh extraheader_patch.json
```

## License

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Copyright (C) 2023 Chad Trabant, EarthScope Data Service
