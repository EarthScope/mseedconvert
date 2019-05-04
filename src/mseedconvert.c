/***************************************************************************
 * mseedconvert.c
 *
 * Convert miniSEED.  For example, miniSEED 2 to 3 or to change encoding.
 *
 * Written by Chad Trabant, IRIS Data Management Center
 ***************************************************************************/

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <libmseed.h>
#include <parson.h>

#define VERSION "0.6"
#define PACKAGE "mseedconvert"

static int8_t verbose = 0;
static int packreclen = -1;
static int packencoding = -1;
static int packversion = 3;
static int8_t forcerepack = 0;
static char *inputfile = NULL;
static char *outputfile = NULL;
static FILE *outfile = NULL;

static char *extraheaderfile = NULL;
static char *extraheader = NULL;
static uint16_t extraheaderlength = 0;

static int extraheader_init (char *file);
static int convertsamples (MS3Record *msr, int packencoding);
static int retired_encoding (int8_t encoding);
static int parameter_proc (int argcount, char **argvec);
static void record_handler (char *record, int reclen, void *ptr);
static void print_stderr (char *message);
static void usage (void);

int
main (int argc, char **argv)
{
  MS3Record *msr = 0;
  char *rawrec = NULL;
  int retcode;
  int reclen;
  uint32_t flags = 0;

  int64_t packedsamples;
  int64_t packedrecords;
  uint64_t totalpackedsamples = 0;
  uint64_t totalpackedrecords = 0;
  int8_t lastrecord;
  int bigendianhost = ms_bigendianhost ();
  int repackheaderV3 = 0;

  /* Process given parameters (command line and parameter file) */
  if (parameter_proc (argc, argv) < 0)
    return -1;

  /* Redirect libmseed logging facility to stderr and set error message prefix */
  ms_loginit (print_stderr, NULL, print_stderr, "ERROR: ");

  /* Open output file if specified, default is STDOUT */
  if (outputfile && strcmp (outputfile, "-"))
  {
    if ((outfile = fopen (outputfile, "wb")) == NULL)
    {
      ms_log (2, "Cannot open output file: %s (%s)\n", outputfile, strerror (errno));

      return 1;
    }
  }

  /* Set flag to skip non-data */
  flags |= MSF_SKIPNOTDATA;

  /* Loop over the input file */
  while ((retcode = ms3_readmsr (&msr, inputfile, NULL, &lastrecord,
                                 flags, verbose)) == MS_NOERROR)
  {
    if (verbose >= 1)
      msr3_print (msr, verbose - 1);

    /* Determine if unpacking data is not needed when converting to version 3 */
    if (forcerepack == 0 && packversion == 3 &&
        (packencoding < 0 || packencoding == msr->encoding))
    {
      /* Steim encodings must be big endian */
      if (msr->encoding == DE_STEIM1 || msr->encoding == DE_STEIM2)
      {
        /* If BE host and swapping not needed, data payload is BE */
        if (bigendianhost && !(msr->swapflag & MSSWAP_PAYLOAD))
          repackheaderV3 = 1;
        /* If LE host and swapping is needed, data payload is BE */
        else if (!bigendianhost && (msr->swapflag & MSSWAP_PAYLOAD))
          repackheaderV3 = 1;
      }

      /* Integer and float encodings must be litte endian */
      else if (msr->encoding == DE_INT16 || msr->encoding == DE_INT32 ||
               msr->encoding == DE_FLOAT32 || msr->encoding == DE_FLOAT64)
      {
        /* If BE host and swapping is needed, data payload is LE */
        if (bigendianhost && (msr->swapflag & MSSWAP_PAYLOAD))
          repackheaderV3 = 1;
        /* If LE host and swapping is not needed, data payload is LE */
        else if (!bigendianhost && !(msr->swapflag & MSSWAP_PAYLOAD))
          repackheaderV3 = 1;
      }

      /* Text (ASCII) encoding does not need repacking */
      else if (msr->encoding == DE_ASCII)
      {
        repackheaderV3 = 1;
      }
    }

    /* Avoid re-packing of data payload if not needed for version 3 output */
    if (packversion == 3 && (repackheaderV3 || msr->samplecnt == 0))
    {
      if (verbose)
        ms_log (1, "Re-packing record without re-packing encoded data payload\n");

      if (!rawrec && (rawrec = (char *)malloc (MAXRECLEN)) == NULL)
      {
        ms_log (2, "Cannot allocate memory for record buffer\n");
        break;
      }

      /* Replace extra headers */
      if (extraheaderlength)
      {
        if (msr->extra)
          free (msr->extra);
        msr->extra       = extraheader;
        msr->extralength = (uint16_t) extraheaderlength;
      }

      /* Re-packed a parsed record into a version 3 header using raw encoded data */
      reclen = msr3_repack_mseed3 (msr, rawrec, MAXRECLEN, verbose);

      if (reclen < 0)
      {
        ms_log (2, "%s: Cannot repack record\n");
        break;
      }

      record_handler (rawrec, reclen, NULL);

      /* Disconnect replacement extra headers from record */
      if (extraheaderlength)
      {
        msr->extra = 0;
        msr->extralength = 0;
      }

      packedsamples = msr->samplecnt;
      packedrecords = 1;
    }
    /* Otherwise, unpack samples and repack record */
    else
    {
      if (verbose)
        ms_log (1, "Re-packing record with decoded data\n");

      msr->numsamples = msr3_unpack_data (msr, verbose);

      if (msr->numsamples < 0)
      {
        ms_log (2, "%s: Cannot unpack data samples\n", msr->sid);
        break;
      }

      /* Replace extra headers */
      if (extraheaderlength)
      {
        if (msr->extra)
          free (msr->extra);
        msr->extra       = extraheader;
        msr->extralength = (uint16_t) extraheaderlength;
      }

      msr->formatversion = packversion;

      if (packreclen >= 0)
        msr->reclen = packreclen;
      else if (msr->formatversion == 3)
        msr->reclen = MAXRECLEN;

      if (retired_encoding ((packencoding >= 0) ? packencoding : msr->encoding))
      {
        ms_log (2, "Packing for encoding %d not allowed, specify supported encoding with -E\n",
                msr->encoding);
        break;
      }

      /* Convert sample type as needed for packencoding */
      if (packencoding >= 0 && msr->encoding != packencoding)
      {
        if (convertsamples (msr, packencoding))
        {
          ms_log (2, "Cannot convert samples for encoding %d\n", packencoding);
          break;
        }
      }

      if (packencoding >= 0)
        msr->encoding = packencoding;

      packedrecords = msr3_pack (msr, &record_handler, NULL, &packedsamples, MSF_FLUSHDATA, verbose);

      /* Disconnect replacement extra headers from record */
      if (extraheaderlength)
      {
        msr->extra = 0;
        msr->extralength = 0;
      }
    }

    if (packedrecords == -1)
      ms_log (2, "Cannot pack records\n");
    else if (verbose >= 2)
      ms_log (1, "Packed %d records\n", packedrecords);

    totalpackedrecords += packedrecords;
    totalpackedsamples += packedsamples;
  }

  if (retcode != MS_ENDOFFILE)
    ms_log (2, "Error reading %s: %s\n", inputfile, ms_errorstr (retcode));

  if (verbose)
    ms_log (0, "Packed %" PRIu64 " samples into %" PRIu64 " records\n",
            totalpackedsamples, totalpackedrecords);

  /* Make sure everything is cleaned up */
  ms3_readmsr (&msr, NULL, NULL, NULL, 0, 0);

  if (rawrec)
    free (rawrec);

  if (outfile)
    fclose (outfile);

  return 0;
} /* End of main() */


/***************************************************************************
 * extraheader_init:
 *
 * Initialize extra header replacement by reading specified file,
 * parsing JSON it contains and forming a miminized version for replacement.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
static int
extraheader_init (char *file)
{
  JSON_Value *rootvalue = NULL;
  size_t extrasize      = 0;

  rootvalue = json_parse_file (file);

  if ( !rootvalue )
  {
    ms_log (3, "Cannot parse extra header file (%s)\n", file);
    return -1;
  }

  extrasize = json_serialization_size (rootvalue);

  if (!extrasize)
  {
    ms_log (2, "Cannot determine new extrea header serialization size\n");
    json_value_free (rootvalue);
    return -1;
  }

  if (extrasize > 65535)
  {
    ms_log (2, "New serialization size exceeds limit of 65,535 bytes: %" PRIu64 "\n",
            (uint64_t)extrasize);
    json_value_free (rootvalue);
    return -1;
  }

  if (extraheader)
    free (extraheader);

  if ((extraheader = (char *)malloc ((extrasize))) == NULL)
  {
    ms_log (2, "Cannot allocate memory for extra header buffer\n");
    json_value_free (rootvalue);
    return -1;
  }

  if (json_serialize_to_buffer (rootvalue, extraheader, extrasize) != JSONSuccess)
  {
    ms_log (2, "Error serializing JSON for new extra headers\n");
    free (extraheader);
    json_value_free (rootvalue);
    return -1;
  }

  extraheaderlength = extrasize - 1;

  if (rootvalue)
    json_value_free (rootvalue);

  return 0;
} /* End of extraheader_init() */

/***************************************************************************
 * convertsamples:
 *
 * Convert samples to type needed for the specified pack encoding.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
static int
convertsamples (MS3Record *msr, int packencoding)
{
  char encodingtype;
  int32_t *idata;
  float *fdata;
  double *ddata;
  int idx;

  if (!msr)
  {
    ms_log (2, "convertsamples: Error, no MS3Record specified!\n");
    return -1;
  }

  /* Determine sample type needed for pack encoding */
  switch (packencoding)
  {
  case DE_ASCII:
    encodingtype = 'a';
    break;
  case DE_INT16:
  case DE_INT32:
  case DE_STEIM1:
  case DE_STEIM2:
    encodingtype = 'i';
    break;
  case DE_FLOAT32:
    encodingtype = 'f';
    break;
  case DE_FLOAT64:
    encodingtype = 'd';
    break;
  default:
    encodingtype = msr->encoding;
    break;
  }

  idata = (int32_t *)msr->datasamples;
  fdata = (float *)msr->datasamples;
  ddata = (double *)msr->datasamples;

  /* Convert sample type if needed */
  if (msr->sampletype != encodingtype)
  {
    if (msr->sampletype == 'a' || encodingtype == 'a')
    {
      ms_log (2, "Error, cannot convert ASCII samples to/from numeric type\n");
      return -1;
    }

    /* Convert to integers */
    else if (encodingtype == 'i')
    {
      if (msr->sampletype == 'f') /* Convert floats to integers with simple rounding */
      {
        for (idx = 0; idx < msr->numsamples; idx++)
        {
          /* Check for loss of sub-integer */
          if ((fdata[idx] - (int32_t)fdata[idx]) > 0.000001)
          {
            ms_log (2, "Warning, Loss of precision when converting floats to integers, loss: %g\n",
                    (fdata[idx] - (int32_t)fdata[idx]));
            return -1;
          }

          idata[idx] = (int32_t) (fdata[idx] + 0.5);
        }
      }
      else if (msr->sampletype == 'd') /* Convert doubles to integers with simple rounding */
      {
        for (idx = 0; idx < msr->numsamples; idx++)
        {
          /* Check for loss of sub-integer */
          if ((ddata[idx] - (int32_t)ddata[idx]) > 0.000001)
          {
            ms_log (2, "Warning, Loss of precision when converting doubles to integers, loss: %g\n",
                    (ddata[idx] - (int32_t)ddata[idx]));
            return -1;
          }

          idata[idx] = (int32_t) (ddata[idx] + 0.5);
        }

        /* Reallocate buffer for reduced size needed */
        if (!(msr->datasamples = realloc (msr->datasamples, (size_t) (msr->numsamples * sizeof (int32_t)))))
        {
          ms_log (2, "Error, cannot re-allocate buffer for sample conversion\n");
          return -1;
        }
      }

      msr->sampletype = 'i';
    }

    /* Convert to floats */
    else if (encodingtype == 'f')
    {
      if (msr->sampletype == 'i') /* Convert integers to floats */
      {
        for (idx = 0; idx < msr->numsamples; idx++)
          fdata[idx] = (float)idata[idx];
      }
      else if (msr->sampletype == 'd') /* Convert doubles to floats */
      {
        for (idx = 0; idx < msr->numsamples; idx++)
          fdata[idx] = (float)ddata[idx];

        /* Reallocate buffer for reduced size needed */
        if (!(msr->datasamples = realloc (msr->datasamples, (size_t) (msr->numsamples * sizeof (float)))))
        {
          ms_log (2, "Error, cannot re-allocate buffer for sample conversion\n");
          return -1;
        }
      }

      msr->sampletype = 'f';
    }

    /* Convert to doubles */
    else if (encodingtype == 'd')
    {
      if (!(ddata = (double *)malloc ((size_t) (msr->numsamples * sizeof (double)))))
      {
        ms_log (2, "Error, cannot allocate buffer for sample conversion to doubles\n");
        return -1;
      }

      if (msr->sampletype == 'i') /* Convert integers to doubles */
      {
        for (idx = 0; idx < msr->numsamples; idx++)
          ddata[idx] = (double)idata[idx];

        free (idata);
      }
      else if (msr->sampletype == 'f') /* Convert floats to doubles */
      {
        for (idx = 0; idx < msr->numsamples; idx++)
          ddata[idx] = (double)fdata[idx];

        free (fdata);
      }

      msr->datasamples = ddata;
      msr->sampletype = 'd';
    }
  }

  return 0;
} /* End of convertsamples() */


/***************************************************************************
 * retired_encoding:
 *
 * Determine if encoding is retired:
 *
 *  2 (24-bit integers)
 *  12 (GEOSCOPE multiplexed format 24-bit integer)
 *  13 (GEOSCOPE multiplexed format 16-bit gain ranged, 3-bit exponent)
 *  14 (GEOSCOPE multiplexed format 16-bit gain ranged, 4-bit exponent)
 *  15 (US National Network compression)
 *  16 (CDSN 16-bit gain ranged)
 *  17 (Graefenberg 16-bit gain ranged)
 *  18 (IPG-Strasbourg 16-bit gain ranged)
 *  30 (SRO format)
 *  31 (HGLP format)
 *  32 (DWWSSN gain ranged format)
 *  33 (RSTN 16-bit gain ranged format)
 *
 * Returns 1 if encoding is retired, otherwise 0.
 ***************************************************************************/
static int
retired_encoding (int8_t encoding)
{

  if (encoding == 2 || encoding == 12 || encoding == 13 ||
      encoding == 14 || encoding == 15 || encoding == 16 ||
      encoding == 17 || encoding == 18 || encoding == 30 ||
      encoding == 31 || encoding == 32 || encoding == 33)
  {
    return 1;
  }

  return 0;
} /* End of retired_encoding() */


/***************************************************************************
 * parameter_proc:
 *
 * Process the command line parameters.
 *
 * Returns 0 on success, and -1 on failure
 ***************************************************************************/
static int
parameter_proc (int argcount, char **argvec)
{
  int optind;

  /* Process all command line arguments */
  for (optind = 1; optind < argcount; optind++)
  {
    if (strcmp (argvec[optind], "-V") == 0)
    {
      ms_log (1, "%s version: %s\n", PACKAGE, VERSION);
      exit (0);
    }
    else if (strcmp (argvec[optind], "-h") == 0)
    {
      usage ();
      exit (0);
    }
    else if (strncmp (argvec[optind], "-v", 2) == 0)
    {
      verbose += strspn (&argvec[optind][1], "v");
    }
    else if (strcmp (argvec[optind], "-f") == 0)
    {
      forcerepack = 1;
    }
    else if (strcmp (argvec[optind], "-R") == 0)
    {
      packreclen = strtol (argvec[++optind], NULL, 10);
    }
    else if (strcmp (argvec[optind], "-E") == 0)
    {
      packencoding = strtol (argvec[++optind], NULL, 10);
    }
    else if (strcmp (argvec[optind], "-F") == 0)
    {
      packversion = strtol (argvec[++optind], NULL, 10);
    }
    else if (strcmp (argvec[optind], "-eh") == 0)
    {
      extraheaderfile = argvec[++optind];
    }
    else if (strcmp (argvec[optind], "-o") == 0)
    {
      outputfile = argvec[++optind];
    }
    else if (strncmp (argvec[optind], "-", 1) == 0 &&
             strlen (argvec[optind]) > 1)
    {
      ms_log (2, "Unknown option: %s\n", argvec[optind]);
      exit (1);
    }
    else if (!inputfile)
    {
      inputfile = argvec[optind];
    }
    else
    {
      ms_log (2, "Unknown option: %s\n", argvec[optind]);
      exit (1);
    }
  }

  /* Make sure an inputfile was specified */
  if (!inputfile)
  {
    ms_log (2, "No input file was specified\n\n");
    ms_log (1, "%s version %s\n\n", PACKAGE, VERSION);
    ms_log (1, "Try %s -h for usage\n", PACKAGE);
    exit (1);
  }

  if (packencoding >= 0 && retired_encoding (packencoding))
  {
    ms_log (2, "Packing for encoding %d not allowed, specify supported encoding with -E\n",
            packencoding);
    exit (1);
  }

  /* Prepare specified replacement extra headers */
  if (extraheaderfile)
  {
    if (extraheader_init (extraheaderfile))
    {
      ms_log (2, "Cannot read extra header file\n");
      exit (1);
    }
  }

  /* Set output to STDOUT if a file is not specified */
  if (!outputfile)
  {
    outfile = stdout;
  }

  /* Report the program version */
  if (verbose)
    ms_log (1, "%s version: %s\n", PACKAGE, VERSION);

  return 0;
} /* End of parameter_proc() */

/***************************************************************************
 * record_handler:
 * Saves passed records to the output file.
 ***************************************************************************/
static void
record_handler (char *record, int reclen, void *ptr)
{
  if (fwrite (record, reclen, 1, outfile) != 1)
  {
    ms_log (2, "Cannot write to output file\n");
  }
} /* End of record_handler() */

/***************************************************************************
 * print_stderr:
 * Print messsage to stderr.
 ***************************************************************************/
static void
print_stderr (char *message)
{
  fprintf (stderr, "%s", message);
} /* End of print_stderr() */

/***************************************************************************
 * usage:
 * Print the usage message and exit.
 ***************************************************************************/
static void
usage (void)
{
  fprintf (stderr, "%s version: %s\n\n", PACKAGE, VERSION);
  fprintf (stderr, "Usage: %s [options] -o outfile infile\n\n", PACKAGE);
  fprintf (stderr,
           " ## Options ##\n"
           " -V             Report program version\n"
           " -h             Show this usage message\n"
           " -v             Be more verbose, multiple flags can be used\n"
           " -f             Force full repack, do not use shortcut\n"
           " -R bytes       Specify record length in bytes for packing\n"
           " -E encoding    Specify encoding format for packing\n"
           " -F version     Specify output format version, default is 3\n"
           " -eh JSONFile   Specify replacement extra headers in JSON format\n"
           "\n"
           " -o outfile     Specify the output file, required\n"
           "\n"
           " infile         Input miniSEED file\n"
           "\n"
           "Each record is converted independently.  This can lead to unfilled records\n"
           "that contain padding depending on the conversion options.\n");
} /* End of usage() */
