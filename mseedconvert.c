
// cc -Wall -O2 libmseed.a -I. mseedconvert.c -o mseedconvert

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

#ifndef WIN32
#include <signal.h>
static void term_handler (int sig);
#endif

#include <libmseed.h>
#include <parson.h>


#define VERSION "0.1"
#define PACKAGE "mseedconvert"

static int8_t verbose = 0;
static int packreclen = -1;
static int packencoding = -1;
static int packversion = 3;
static int8_t forcerepack = 0;
static char *inputfile = NULL;
static char *outputfile = NULL;
static FILE *outfile = NULL;

static char *json_input = NULL;


static int convertsamples (MS3Record *msr, int packencoding);
static int parameter_proc (int argcount, char **argvec);
static void record_handler (char *record, int reclen, void *ptr);
static void print_stderr (char *message);
static void usage (void);
static void term_handler (int sig);

int jsonInjectionSetup(MS3Record *msr,int verbose);


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

#ifndef WIN32
  /* Signal handling, use POSIX calls with standardized semantics */
  struct sigaction sa;

  sa.sa_flags = SA_RESTART;
  sigemptyset (&sa.sa_mask);

  sa.sa_handler = term_handler;
  sigaction (SIGINT, &sa, NULL);
  sigaction (SIGQUIT, &sa, NULL);
  sigaction (SIGTERM, &sa, NULL);

  sa.sa_handler = SIG_IGN;
  sigaction (SIGHUP, &sa, NULL);
  sigaction (SIGPIPE, &sa, NULL);
#endif

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

    //TODO the shortcut can only be used for Steim[12], i.e. when encoding is known to be transportable
    // others, i.e. ints, floats, can be the wrong byte order.

    // Test for litte-endian Steim[12] before converting without re-encoding

    // If the datapayload byte order is known it could be swapped here?

    /* Conversion to version 3, if unpacking data is not needed */
    if (forcerepack == 0 && packversion == 3 &&
        (packencoding < 0 || packencoding == msr->encoding || msr->samplecnt == 0))
    {
      if (!rawrec && (rawrec = (char *)malloc (MAXRECLEN)) == NULL)
      {
        ms_log (2, "Cannot allocate memory for record buffer\n");
        break;
      }

      // TODO repack could determine the number of samples for INT, FLOAT32 and FLOAT64 and trim payload length

     //Inject json
     if(json_input != NULL)
     {
        jsonInjectionSetup(msr,verbose);
     }

      reclen = msr3_repack_mseed3 (msr, rawrec, MAXRECLEN, verbose);

      if (reclen < 0)
      {
        ms_log (2, "%s: Cannot repack record\n");
        break;
      }

      record_handler (rawrec, reclen, NULL);

      packedsamples = msr->samplecnt;
      packedrecords = 1;
    }
    /* Otherwise, unpack samples and repack record */
    else
    {
      msr->numsamples = msr3_unpack_data (msr, verbose);

      if (msr->numsamples < 0)
      {
        ms_log (2, "%s: Cannot unpack data samples\n", msr->tsid);
        break;
      }

      /* Convert sample type as needed for packencoding */
      if (convertsamples (msr, packencoding))
      {
        ms_log (2, "Error converting samples for encoding %d\n", packencoding);
        break;
      }

     //Inject json
     if(json_input != NULL)
     {
       jsonInjectionSetup(msr,verbose);
     }

      msr->formatversion = packversion;

      if (packreclen >= 0)
        msr->reclen = packreclen;
      else if (msr->formatversion == 3)
        msr->reclen = MAXRECLEN;

      if (packencoding >= 0)
        msr->encoding = packencoding;

      packedrecords = msr3_pack (msr, &record_handler, NULL, &packedsamples, MSF_FLUSHDATA, verbose);
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



//Inject json routine
int jsonInjectionSetup(MS3Record *msr, int verbose)
{
    size_t extra_size = 0;
    char * extra_buf = NULL;
    char * pretty_json = NULL;
    JSON_Value * root_value = NULL;

    ms_log(0,"Input JSON file found, serializing for injection into mseed record\n");
    root_value = json_parse_file(json_input);


    if(verbose==3)
    {
        pretty_json = json_serialize_to_string_pretty(root_value);
        printf("Root element:\n  %s\n",pretty_json);
        json_free_serialized_string(pretty_json);
    }


    extra_size =  json_serialization_size(root_value);


    printf("extra buffer size = %zu\n",extra_size);

    if ((extra_buf = (char *)malloc ((extra_size))) == NULL)
    {
        ms_log (2, "Cannot allocate memory for extra header buffer\n");

    }

    JSON_Status ierr = json_serialize_to_buffer(root_value, extra_buf, extra_size);

    //Debug
    //printf("extra buffer:\n%s\n",extra_buf);

    if(ierr == JSONFailure)
    {
        ms_log (2, "Cannot serialize json to buffer: %s\n", json_input);
        printf("error code = %d\n",ierr);
        return 1;
    }

    msr->extralength = (uint16_t)(extra_size-1);
    msr->extra = extra_buf;
    int32_t recl = msr->reclen;
    recl = recl + (int32_t)extra_size;
    msr->reclen = recl;

    if(root_value)
    {
        json_value_free(root_value);
    }

    return 0;

}




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
    else if (strcmp (argvec[optind], "-j") == 0)
    {
      json_input = argvec[++optind];
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

    ms_log (1, "input file is %s\n", inputfile);

    if(json_input)
        ms_log (1, "JSON file is %s\n", json_input);

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
           " -j jsonFile    Specify input json header to inject\n"
           "\n"
           " -o outfile     Specify the output file, required\n"
           "\n"
           " infile         Input miniSEED file\n"
           "\n"
           "Each record is converted independently.  This can lead to unfilled records\n"
           "that contain padding depending on the conversion options.\n");
} /* End of usage() */

#ifndef WIN32
/***************************************************************************
 * term_handler:
 * Signal handler routine.
 ***************************************************************************/
static void
term_handler (int sig)
{
  exit (0);
}
#endif
