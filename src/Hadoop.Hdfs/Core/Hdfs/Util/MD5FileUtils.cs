using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>
	/// Static functions for dealing with files of the same format
	/// that the Unix "md5sum" utility writes.
	/// </summary>
	public abstract class MD5FileUtils
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(MD5FileUtils));

		public const string Md5Suffix = ".md5";

		private static readonly Sharpen.Pattern LineRegex = Sharpen.Pattern.Compile("([0-9a-f]{32}) [ \\*](.+)"
			);

		/// <summary>
		/// Verify that the previously saved md5 for the given file matches
		/// expectedMd5.
		/// </summary>
		/// <exception cref="System.IO.IOException"></exception>
		public static void VerifySavedMD5(FilePath dataFile, MD5Hash expectedMD5)
		{
			MD5Hash storedHash = ReadStoredMd5ForFile(dataFile);
			// Check the hash itself
			if (!expectedMD5.Equals(storedHash))
			{
				throw new IOException("File " + dataFile + " did not match stored MD5 checksum " 
					+ " (stored: " + storedHash + ", computed: " + expectedMD5);
			}
		}

		/// <summary>
		/// Read the md5 file stored alongside the given data file
		/// and match the md5 file content.
		/// </summary>
		/// <param name="dataFile">the file containing data</param>
		/// <returns>
		/// a matcher with two matched groups
		/// where group(1) is the md5 string and group(2) is the data file path.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		private static Matcher ReadStoredMd5(FilePath md5File)
		{
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream
				(md5File), Charsets.Utf8));
			string md5Line;
			try
			{
				md5Line = reader.ReadLine();
				if (md5Line == null)
				{
					md5Line = string.Empty;
				}
				md5Line = md5Line.Trim();
			}
			catch (IOException ioe)
			{
				throw new IOException("Error reading md5 file at " + md5File, ioe);
			}
			finally
			{
				IOUtils.Cleanup(Log, reader);
			}
			Matcher matcher = LineRegex.Matcher(md5Line);
			if (!matcher.Matches())
			{
				throw new IOException("Invalid MD5 file " + md5File + ": the content \"" + md5Line
					 + "\" does not match the expected pattern.");
			}
			return matcher;
		}

		/// <summary>Read the md5 checksum stored alongside the given data file.</summary>
		/// <param name="dataFile">the file containing data</param>
		/// <returns>the checksum stored in dataFile.md5</returns>
		/// <exception cref="System.IO.IOException"/>
		public static MD5Hash ReadStoredMd5ForFile(FilePath dataFile)
		{
			FilePath md5File = GetDigestFileForFile(dataFile);
			if (!md5File.Exists())
			{
				return null;
			}
			Matcher matcher = ReadStoredMd5(md5File);
			string storedHash = matcher.Group(1);
			FilePath referencedFile = new FilePath(matcher.Group(2));
			// Sanity check: Make sure that the file referenced in the .md5 file at
			// least has the same name as the file we expect
			if (!referencedFile.GetName().Equals(dataFile.GetName()))
			{
				throw new IOException("MD5 file at " + md5File + " references file named " + referencedFile
					.GetName() + " but we expected it to reference " + dataFile);
			}
			return new MD5Hash(storedHash);
		}

		/// <summary>Read dataFile and compute its MD5 checksum.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static MD5Hash ComputeMd5ForFile(FilePath dataFile)
		{
			InputStream @in = new FileInputStream(dataFile);
			try
			{
				MessageDigest digester = MD5Hash.GetDigester();
				DigestInputStream dis = new DigestInputStream(@in, digester);
				IOUtils.CopyBytes(dis, new IOUtils.NullOutputStream(), 128 * 1024);
				return new MD5Hash(digester.Digest());
			}
			finally
			{
				IOUtils.CloseStream(@in);
			}
		}

		/// <summary>Save the ".md5" file that lists the md5sum of another file.</summary>
		/// <param name="dataFile">the original file whose md5 was computed</param>
		/// <param name="digest">the computed digest</param>
		/// <exception cref="System.IO.IOException"/>
		public static void SaveMD5File(FilePath dataFile, MD5Hash digest)
		{
			string digestString = StringUtils.ByteToHexString(digest.GetDigest());
			SaveMD5File(dataFile, digestString);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void SaveMD5File(FilePath dataFile, string digestString)
		{
			FilePath md5File = GetDigestFileForFile(dataFile);
			string md5Line = digestString + " *" + dataFile.GetName() + "\n";
			AtomicFileOutputStream afos = new AtomicFileOutputStream(md5File);
			afos.Write(Sharpen.Runtime.GetBytesForString(md5Line, Charsets.Utf8));
			afos.Close();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Saved MD5 " + digestString + " to " + md5File);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void RenameMD5File(FilePath oldDataFile, FilePath newDataFile)
		{
			FilePath fromFile = GetDigestFileForFile(oldDataFile);
			if (!fromFile.Exists())
			{
				throw new FileNotFoundException(fromFile + " does not exist.");
			}
			string digestString = ReadStoredMd5(fromFile).Group(1);
			SaveMD5File(newDataFile, digestString);
			if (!fromFile.Delete())
			{
				Log.Warn("deleting  " + fromFile.GetAbsolutePath() + " FAILED");
			}
		}

		/// <returns>
		/// a reference to the file with .md5 suffix that will
		/// contain the md5 checksum for the given data file.
		/// </returns>
		public static FilePath GetDigestFileForFile(FilePath file)
		{
			return new FilePath(file.GetParentFile(), file.GetName() + Md5Suffix);
		}
	}
}
