/*
* Licensed to the Apache Software Foundation (ASF) under one
*  or more contributor license agreements.  See the NOTICE file
*  distributed with this work for additional information
*  regarding copyright ownership.  The ASF licenses this file
*  to you under the Apache License, Version 2.0 (the
*  "License"); you may not use this file except in compliance
*  with the License.  You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/
using System;
using System.IO;
using System.Text;
using NUnit.Framework;
using NUnit.Framework.Internal;
using Org.Apache.Hadoop.FS;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Contract
{
	/// <summary>Utilities used across test cases</summary>
	public class ContractTestUtils : Assert
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(ContractTestUtils
			));

		public const string IoFileBufferSize = "io.file.buffer.size";

		public const string IoChunkBufferSize = "io.chunk.buffer.size";

		public const int DefaultIoChunkBufferSize = 128;

		public const string IoChunkModulusSize = "io.chunk.modulus.size";

		public const int DefaultIoChunkModulusSize = 128;

		// For scale testing, we can repeatedly write small chunk data to generate
		// a large file.
		/// <summary>Assert that a property in the property set matches the expected value</summary>
		/// <param name="props">property set</param>
		/// <param name="key">property name</param>
		/// <param name="expected">expected value. If null, the property must not be in the set
		/// 	</param>
		public static void AssertPropertyEquals(Properties props, string key, string expected
			)
		{
			string val = props.GetProperty(key);
			if (expected == null)
			{
				NUnit.Framework.Assert.IsNull("Non null property " + key + " = " + val, val);
			}
			else
			{
				Assert.Equal("property " + key + " = " + val, expected, val);
			}
		}

		/// <summary>Write a file and read it in, validating the result.</summary>
		/// <remarks>
		/// Write a file and read it in, validating the result. Optional flags control
		/// whether file overwrite operations should be enabled, and whether the
		/// file should be deleted afterwards.
		/// If there is a mismatch between what was written and what was expected,
		/// a small range of bytes either side of the first error are logged to aid
		/// diagnosing what problem occurred -whether it was a previous file
		/// or a corrupting of the current file. This assumes that two
		/// sequential runs to the same path use datasets with different character
		/// moduli.
		/// </remarks>
		/// <param name="fs">filesystem</param>
		/// <param name="path">path to write to</param>
		/// <param name="len">length of data</param>
		/// <param name="overwrite">should the create option allow overwrites?</param>
		/// <param name="delete">
		/// should the file be deleted afterwards? -with a verification
		/// that it worked. Deletion is not attempted if an assertion has failed
		/// earlier -it is not in a <code>finally{}</code> block.
		/// </param>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public static void WriteAndRead(FileSystem fs, Path path, byte[] src, int len, int
			 blocksize, bool overwrite, bool delete)
		{
			fs.Mkdirs(path.GetParent());
			WriteDataset(fs, path, src, len, blocksize, overwrite);
			byte[] dest = ReadDataset(fs, path, len);
			CompareByteArrays(src, dest, len);
			if (delete)
			{
				RejectRootOperation(path);
				bool deleted = fs.Delete(path, false);
				Assert.True("Deleted", deleted);
				AssertPathDoesNotExist(fs, "Cleanup failed", path);
			}
		}

		/// <summary>Write a file.</summary>
		/// <remarks>
		/// Write a file.
		/// Optional flags control
		/// whether file overwrite operations should be enabled
		/// </remarks>
		/// <param name="fs">filesystem</param>
		/// <param name="path">path to write to</param>
		/// <param name="len">length of data</param>
		/// <param name="overwrite">should the create option allow overwrites?</param>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public static void WriteDataset(FileSystem fs, Path path, byte[] src, int len, int
			 buffersize, bool overwrite)
		{
			Assert.True("Not enough data in source array to write " + len +
				 " bytes", src.Length >= len);
			FSDataOutputStream @out = fs.Create(path, overwrite, fs.GetConf().GetInt(IoFileBufferSize
				, 4096), (short)1, buffersize);
			@out.Write(src, 0, len);
			@out.Close();
			AssertFileHasLength(fs, path, len);
		}

		/// <summary>Read the file and convert to a byte dataset.</summary>
		/// <remarks>
		/// Read the file and convert to a byte dataset.
		/// This implements readfully internally, so that it will read
		/// in the file without ever having to seek()
		/// </remarks>
		/// <param name="fs">filesystem</param>
		/// <param name="path">path to read from</param>
		/// <param name="len">length of data to read</param>
		/// <returns>the bytes</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public static byte[] ReadDataset(FileSystem fs, Path path, int len)
		{
			FSDataInputStream @in = fs.Open(path);
			byte[] dest = new byte[len];
			int offset = 0;
			int nread = 0;
			try
			{
				while (nread < len)
				{
					int nbytes = @in.Read(dest, offset + nread, len - nread);
					if (nbytes < 0)
					{
						throw new EOFException("End of file reached before reading fully.");
					}
					nread += nbytes;
				}
			}
			finally
			{
				@in.Close();
			}
			return dest;
		}

		/// <summary>Read a file, verify its length and contents match the expected array</summary>
		/// <param name="fs">filesystem</param>
		/// <param name="path">path to file</param>
		/// <param name="original">original dataset</param>
		/// <exception cref="System.IO.IOException">IO Problems</exception>
		public static void VerifyFileContents(FileSystem fs, Path path, byte[] original)
		{
			FileStatus stat = fs.GetFileStatus(path);
			string statText = stat.ToString();
			Assert.True("not a file " + statText, stat.IsFile());
			Assert.Equal("wrong length " + statText, original.Length, stat
				.GetLen());
			byte[] bytes = ReadDataset(fs, path, original.Length);
			CompareByteArrays(original, bytes, original.Length);
		}

		/// <summary>
		/// Verify that the read at a specific offset in a stream
		/// matches that expected
		/// </summary>
		/// <param name="stm">stream</param>
		/// <param name="fileContents">original file contents</param>
		/// <param name="seekOff">seek offset</param>
		/// <param name="toRead">number of bytes to read</param>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public static void VerifyRead(FSDataInputStream stm, byte[] fileContents, int seekOff
			, int toRead)
		{
			byte[] @out = new byte[toRead];
			stm.Seek(seekOff);
			stm.ReadFully(@out);
			byte[] expected = Arrays.CopyOfRange(fileContents, seekOff, seekOff + toRead);
			CompareByteArrays(expected, @out, toRead);
		}

		/// <summary>Assert that tthe array original[0..len] and received[] are equal.</summary>
		/// <remarks>
		/// Assert that tthe array original[0..len] and received[] are equal.
		/// A failure triggers the logging of the bytes near where the first
		/// difference surfaces.
		/// </remarks>
		/// <param name="original">source data</param>
		/// <param name="received">actual</param>
		/// <param name="len">length of bytes to compare</param>
		public static void CompareByteArrays(byte[] original, byte[] received, int len)
		{
			Assert.Equal("Number of bytes read != number written", len, received
				.Length);
			int errors = 0;
			int first_error_byte = -1;
			for (int i = 0; i < len; i++)
			{
				if (original[i] != received[i])
				{
					if (errors == 0)
					{
						first_error_byte = i;
					}
					errors++;
				}
			}
			if (errors > 0)
			{
				string message = string.Format(" %d errors in file of length %d", errors, len);
				Log.Warn(message);
				// the range either side of the first error to print
				// this is a purely arbitrary number, to aid user debugging
				int overlap = 10;
				for (int i_1 = Math.Max(0, first_error_byte - overlap); i_1 < Math.Min(first_error_byte
					 + overlap, len); i_1++)
				{
					byte actual = received[i_1];
					byte expected = original[i_1];
					string letter = ToChar(actual);
					string line = string.Format("[%04d] %2x %s\n", i_1, actual, letter);
					if (expected != actual)
					{
						line = string.Format("[%04d] %2x %s -expected %2x %s\n", i_1, actual, letter, expected
							, ToChar(expected));
					}
					Log.Warn(line);
				}
				NUnit.Framework.Assert.Fail(message);
			}
		}

		/// <summary>Convert a byte to a character for printing.</summary>
		/// <remarks>
		/// Convert a byte to a character for printing. If the
		/// byte value is &lt; 32 -and hence unprintable- the byte is
		/// returned as a two digit hex value
		/// </remarks>
		/// <param name="b">byte</param>
		/// <returns>the printable character string</returns>
		public static string ToChar(byte b)
		{
			if (b >= unchecked((int)(0x20)))
			{
				return char.ToString((char)b);
			}
			else
			{
				return string.Format("%02x", b);
			}
		}

		/// <summary>Convert a buffer to a string, character by character</summary>
		/// <param name="buffer">input bytes</param>
		/// <returns>a string conversion</returns>
		public static string ToChar(byte[] buffer)
		{
			StringBuilder builder = new StringBuilder(buffer.Length);
			foreach (byte b in buffer)
			{
				builder.Append(ToChar(b));
			}
			return builder.ToString();
		}

		public static byte[] ToAsciiByteArray(string s)
		{
			char[] chars = s.ToCharArray();
			int len = chars.Length;
			byte[] buffer = new byte[len];
			for (int i = 0; i < len; i++)
			{
				buffer[i] = unchecked((byte)(chars[i] & unchecked((int)(0xff))));
			}
			return buffer;
		}

		/// <summary>Cleanup at the end of a test run</summary>
		/// <param name="action">action triggering the operation (for use in logging)</param>
		/// <param name="fileSystem">filesystem to work with. May be null</param>
		/// <param name="cleanupPath">path to delete as a string</param>
		public static void Cleanup(string action, FileSystem fileSystem, string cleanupPath
			)
		{
			if (fileSystem == null)
			{
				return;
			}
			Path path = new Path(cleanupPath).MakeQualified(fileSystem.GetUri(), fileSystem.GetWorkingDirectory
				());
			Cleanup(action, fileSystem, path);
		}

		/// <summary>Cleanup at the end of a test run</summary>
		/// <param name="action">action triggering the operation (for use in logging)</param>
		/// <param name="fileSystem">filesystem to work with. May be null</param>
		/// <param name="path">path to delete</param>
		public static void Cleanup(string action, FileSystem fileSystem, Path path)
		{
			NoteAction(action);
			try
			{
				Rm(fileSystem, path, true, false);
			}
			catch (Exception e)
			{
				Log.Error("Error deleting in " + action + " - " + path + ": " + e, e);
			}
		}

		/// <summary>Delete a directory.</summary>
		/// <remarks>
		/// Delete a directory. There's a safety check for operations against the
		/// root directory -these are intercepted and rejected with an IOException
		/// unless the allowRootDelete flag is true
		/// </remarks>
		/// <param name="fileSystem">filesystem to work with. May be null</param>
		/// <param name="path">path to delete</param>
		/// <param name="recursive">flag to enable recursive delete</param>
		/// <param name="allowRootDelete">can the root directory be deleted?</param>
		/// <exception cref="System.IO.IOException">on any problem.</exception>
		public static bool Rm(FileSystem fileSystem, Path path, bool recursive, bool allowRootDelete
			)
		{
			if (fileSystem != null)
			{
				RejectRootOperation(path, allowRootDelete);
				if (fileSystem.Exists(path))
				{
					return fileSystem.Delete(path, recursive);
				}
			}
			return false;
		}

		/// <summary>Block any operation on the root path.</summary>
		/// <remarks>Block any operation on the root path. This is a safety check</remarks>
		/// <param name="path">path in the filesystem</param>
		/// <param name="allowRootOperation">can the root directory be manipulated?</param>
		/// <exception cref="System.IO.IOException">if the operation was rejected</exception>
		public static void RejectRootOperation(Path path, bool allowRootOperation)
		{
			if (path.IsRoot() && !allowRootOperation)
			{
				throw new IOException("Root directory operation rejected: " + path);
			}
		}

		/// <summary>Block any operation on the root path.</summary>
		/// <remarks>Block any operation on the root path. This is a safety check</remarks>
		/// <param name="path">path in the filesystem</param>
		/// <exception cref="System.IO.IOException">if the operation was rejected</exception>
		public static void RejectRootOperation(Path path)
		{
			RejectRootOperation(path, false);
		}

		public static void NoteAction(string action)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("==============  " + action + " =============");
			}
		}

		/// <summary>
		/// downgrade a failure to a message and a warning, then an
		/// exception for the Junit test runner to mark as failed
		/// </summary>
		/// <param name="message">text message</param>
		/// <param name="failure">what failed</param>
		/// <exception cref="NUnit.Framework.Internal.AssumptionViolatedException">always</exception>
		public static void Downgrade(string message, Exception failure)
		{
			Log.Warn("Downgrading test " + message, failure);
			AssumptionViolatedException ave = new AssumptionViolatedException(failure, null);
			throw ave;
		}

		/// <summary>report an overridden test as unsupported</summary>
		/// <param name="message">message to use in the text</param>
		/// <exception cref="NUnit.Framework.Internal.AssumptionViolatedException">always</exception>
		public static void Unsupported(string message)
		{
			Skip(message);
		}

		/// <summary>report a test has been skipped for some reason</summary>
		/// <param name="message">message to use in the text</param>
		/// <exception cref="NUnit.Framework.Internal.AssumptionViolatedException">always</exception>
		public static void Skip(string message)
		{
			Log.Info("Skipping: {}", message);
			throw new AssumptionViolatedException(message);
		}

		/// <summary>Fail with an exception that was received</summary>
		/// <param name="text">text to use in the exception</param>
		/// <param name="thrown">a (possibly null) throwable to init the cause with</param>
		/// <exception cref="System.Exception">with the text and throwable -always</exception>
		public static void Fail(string text, Exception thrown)
		{
			Exception e = new Exception(text);
			Sharpen.Extensions.InitCause(e, thrown);
			throw e;
		}

		/// <summary>Make an assertion about the length of a file</summary>
		/// <param name="fs">filesystem</param>
		/// <param name="path">path of the file</param>
		/// <param name="expected">expected length</param>
		/// <exception cref="System.IO.IOException">on File IO problems</exception>
		public static void AssertFileHasLength(FileSystem fs, Path path, int expected)
		{
			FileStatus status = fs.GetFileStatus(path);
			Assert.Equal("Wrong file length of file " + path + " status: "
				 + status, expected, status.GetLen());
		}

		/// <summary>Assert that a path refers to a directory</summary>
		/// <param name="fs">filesystem</param>
		/// <param name="path">path of the directory</param>
		/// <exception cref="System.IO.IOException">on File IO problems</exception>
		public static void AssertIsDirectory(FileSystem fs, Path path)
		{
			FileStatus fileStatus = fs.GetFileStatus(path);
			AssertIsDirectory(fileStatus);
		}

		/// <summary>Assert that a path refers to a directory</summary>
		/// <param name="fileStatus">stats to check</param>
		public static void AssertIsDirectory(FileStatus fileStatus)
		{
			Assert.True("Should be a directory -but isn't: " + fileStatus, 
				fileStatus.IsDirectory());
		}

		/// <summary>
		/// Write the text to a file, returning the converted byte array
		/// for use in validating the round trip
		/// </summary>
		/// <param name="fs">filesystem</param>
		/// <param name="path">path of file</param>
		/// <param name="text">text to write</param>
		/// <param name="overwrite">should the operation overwrite any existing file?</param>
		/// <returns>the read bytes</returns>
		/// <exception cref="System.IO.IOException">on IO problems</exception>
		public static byte[] WriteTextFile(FileSystem fs, Path path, string text, bool overwrite
			)
		{
			byte[] bytes = new byte[0];
			if (text != null)
			{
				bytes = ToAsciiByteArray(text);
			}
			CreateFile(fs, path, overwrite, bytes);
			return bytes;
		}

		/// <summary>Create a file</summary>
		/// <param name="fs">filesystem</param>
		/// <param name="path">path to write</param>
		/// <param name="overwrite">overwrite flag</param>
		/// <param name="data">source dataset. Can be null</param>
		/// <exception cref="System.IO.IOException">on any problem</exception>
		public static void CreateFile(FileSystem fs, Path path, bool overwrite, byte[] data
			)
		{
			FSDataOutputStream stream = fs.Create(path, overwrite);
			if (data != null && data.Length > 0)
			{
				stream.Write(data);
			}
			stream.Close();
		}

		/// <summary>Touch a file</summary>
		/// <param name="fs">filesystem</param>
		/// <param name="path">path</param>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public static void Touch(FileSystem fs, Path path)
		{
			CreateFile(fs, path, true, null);
		}

		/// <summary>
		/// Delete a file/dir and assert that delete() returned true
		/// <i>and</i> that the path no longer exists.
		/// </summary>
		/// <remarks>
		/// Delete a file/dir and assert that delete() returned true
		/// <i>and</i> that the path no longer exists. This variant rejects
		/// all operations on root directories
		/// </remarks>
		/// <param name="fs">filesystem</param>
		/// <param name="file">path to delete</param>
		/// <param name="recursive">flag to enable recursive delete</param>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public static void AssertDeleted(FileSystem fs, Path file, bool recursive)
		{
			AssertDeleted(fs, file, recursive, false);
		}

		/// <summary>
		/// Delete a file/dir and assert that delete() returned true
		/// <i>and</i> that the path no longer exists.
		/// </summary>
		/// <remarks>
		/// Delete a file/dir and assert that delete() returned true
		/// <i>and</i> that the path no longer exists. This variant rejects
		/// all operations on root directories
		/// </remarks>
		/// <param name="fs">filesystem</param>
		/// <param name="file">path to delete</param>
		/// <param name="recursive">flag to enable recursive delete</param>
		/// <param name="allowRootOperations">can the root dir be deleted?</param>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public static void AssertDeleted(FileSystem fs, Path file, bool recursive, bool allowRootOperations
			)
		{
			RejectRootOperation(file, allowRootOperations);
			AssertPathExists(fs, "about to be deleted file", file);
			bool deleted = fs.Delete(file, recursive);
			string dir = Ls(fs, file.GetParent());
			Assert.True("Delete failed on " + file + ": " + dir, deleted);
			AssertPathDoesNotExist(fs, "Deleted file", file);
		}

		/// <summary>Read in "length" bytes, convert to an ascii string</summary>
		/// <param name="fs">filesystem</param>
		/// <param name="path">path to read</param>
		/// <param name="length">#of bytes to read.</param>
		/// <returns>the bytes read and converted to a string</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public static string ReadBytesToString(FileSystem fs, Path path, int length)
		{
			FSDataInputStream @in = fs.Open(path);
			try
			{
				byte[] buf = new byte[length];
				@in.ReadFully(0, buf);
				return ToChar(buf);
			}
			finally
			{
				@in.Close();
			}
		}

		/// <summary>Take an array of filestats and convert to a string (prefixed w/ a [01] counter
		/// 	</summary>
		/// <param name="stats">array of stats</param>
		/// <param name="separator">separator after every entry</param>
		/// <returns>a stringified set</returns>
		public static string FileStatsToString(FileStatus[] stats, string separator)
		{
			StringBuilder buf = new StringBuilder(stats.Length * 128);
			for (int i = 0; i < stats.Length; i++)
			{
				buf.Append(string.Format("[%02d] %s", i, stats[i])).Append(separator);
			}
			return buf.ToString();
		}

		/// <summary>List a directory</summary>
		/// <param name="fileSystem">FS</param>
		/// <param name="path">path</param>
		/// <returns>a directory listing or failure message</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string Ls(FileSystem fileSystem, Path path)
		{
			if (path == null)
			{
				//surfaces when someone calls getParent() on something at the top of the path
				return "/";
			}
			FileStatus[] stats;
			string pathtext = "ls " + path;
			try
			{
				stats = fileSystem.ListStatus(path);
			}
			catch (FileNotFoundException)
			{
				return pathtext + " -file not found";
			}
			catch (IOException e)
			{
				return pathtext + " -failed: " + e;
			}
			return DumpStats(pathtext, stats);
		}

		public static string DumpStats(string pathname, FileStatus[] stats)
		{
			return pathname + FileStatsToString(stats, "\n");
		}

		/// <summary>
		/// Assert that a file exists and whose
		/// <see cref="Org.Apache.Hadoop.FS.FileStatus"/>
		/// entry
		/// declares that this is a file and not a symlink or directory.
		/// </summary>
		/// <param name="fileSystem">filesystem to resolve path against</param>
		/// <param name="filename">name of the file</param>
		/// <exception cref="System.IO.IOException">IO problems during file operations</exception>
		public static void AssertIsFile(FileSystem fileSystem, Path filename)
		{
			AssertPathExists(fileSystem, "Expected file", filename);
			FileStatus status = fileSystem.GetFileStatus(filename);
			AssertIsFile(filename, status);
		}

		/// <summary>
		/// Assert that a file exists and whose
		/// <see cref="Org.Apache.Hadoop.FS.FileStatus"/>
		/// entry
		/// declares that this is a file and not a symlink or directory.
		/// </summary>
		/// <param name="filename">name of the file</param>
		/// <param name="status">file status</param>
		public static void AssertIsFile(Path filename, FileStatus status)
		{
			string fileInfo = filename + "  " + status;
			NUnit.Framework.Assert.IsFalse("File claims to be a directory " + fileInfo, status
				.IsDirectory());
			NUnit.Framework.Assert.IsFalse("File claims to be a symlink " + fileInfo, status.
				IsSymlink());
		}

		/// <summary>
		/// Create a dataset for use in the tests; all data is in the range
		/// base to (base+modulo-1) inclusive
		/// </summary>
		/// <param name="len">length of data</param>
		/// <param name="base">base of the data</param>
		/// <param name="modulo">the modulo</param>
		/// <returns>the newly generated dataset</returns>
		public static byte[] Dataset(int len, int @base, int modulo)
		{
			byte[] dataset = new byte[len];
			for (int i = 0; i < len; i++)
			{
				dataset[i] = unchecked((byte)(@base + (i % modulo)));
			}
			return dataset;
		}

		/// <summary>
		/// Assert that a path exists -but make no assertions as to the
		/// type of that entry
		/// </summary>
		/// <param name="fileSystem">filesystem to examine</param>
		/// <param name="message">message to include in the assertion failure message</param>
		/// <param name="path">path in the filesystem</param>
		/// <exception cref="System.IO.FileNotFoundException">raised if the path is missing</exception>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public static void AssertPathExists(FileSystem fileSystem, string message, Path path
			)
		{
			if (!fileSystem.Exists(path))
			{
				//failure, report it
				Ls(fileSystem, path.GetParent());
				throw new FileNotFoundException(message + ": not found " + path + " in " + path.GetParent
					());
			}
		}

		/// <summary>Assert that a path does not exist</summary>
		/// <param name="fileSystem">filesystem to examine</param>
		/// <param name="message">message to include in the assertion failure message</param>
		/// <param name="path">path in the filesystem</param>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public static void AssertPathDoesNotExist(FileSystem fileSystem, string message, 
			Path path)
		{
			try
			{
				FileStatus status = fileSystem.GetFileStatus(path);
				NUnit.Framework.Assert.Fail(message + ": unexpectedly found " + path + " as  " + 
					status);
			}
			catch (FileNotFoundException)
			{
			}
		}

		//this is expected
		/// <summary>Assert that a FileSystem.listStatus on a dir finds the subdir/child entry
		/// 	</summary>
		/// <param name="fs">filesystem</param>
		/// <param name="dir">directory to scan</param>
		/// <param name="subdir">full path to look for</param>
		/// <exception cref="System.IO.IOException">IO probles</exception>
		public static void AssertListStatusFinds(FileSystem fs, Path dir, Path subdir)
		{
			FileStatus[] stats = fs.ListStatus(dir);
			bool found = false;
			StringBuilder builder = new StringBuilder();
			foreach (FileStatus stat in stats)
			{
				builder.Append(stat.ToString()).Append('\n');
				if (stat.GetPath().Equals(subdir))
				{
					found = true;
				}
			}
			Assert.True("Path " + subdir + " not found in directory " + dir
				 + ":" + builder, found);
		}

		/// <summary>Test for the host being an OSX machine</summary>
		/// <returns>true if the JVM thinks that is running on OSX</returns>
		public static bool IsOSX()
		{
			return Runtime.GetProperty("os.name").Contains("OS X");
		}

		/// <summary>compare content of file operations using a double byte array</summary>
		/// <param name="concat">concatenated files</param>
		/// <param name="bytes">bytes</param>
		public static void ValidateFileContent(byte[] concat, byte[][] bytes)
		{
			int idx = 0;
			bool mismatch = false;
			foreach (byte[] bb in bytes)
			{
				foreach (byte b in bb)
				{
					if (b != concat[idx++])
					{
						mismatch = true;
						break;
					}
				}
				if (mismatch)
				{
					break;
				}
			}
			NUnit.Framework.Assert.IsFalse("File content of file is not as expected at offset "
				 + idx, mismatch);
		}

		/// <summary>
		/// Receives test data from the given input file and checks the size of the
		/// data as well as the pattern inside the received data.
		/// </summary>
		/// <param name="fs">FileSystem</param>
		/// <param name="path">Input file to be checked</param>
		/// <param name="expectedSize">
		/// the expected size of the data to be read from the
		/// input file in bytes
		/// </param>
		/// <param name="bufferLen">Pattern length</param>
		/// <param name="modulus">Pattern modulus</param>
		/// <exception cref="System.IO.IOException">thrown if an error occurs while reading the data
		/// 	</exception>
		public static void VerifyReceivedData(FileSystem fs, Path path, long expectedSize
			, int bufferLen, int modulus)
		{
			byte[] testBuffer = new byte[bufferLen];
			long totalBytesRead = 0;
			int nextExpectedNumber = 0;
			InputStream inputStream = fs.Open(path);
			try
			{
				while (true)
				{
					int bytesRead = inputStream.Read(testBuffer);
					if (bytesRead < 0)
					{
						break;
					}
					totalBytesRead += bytesRead;
					for (int i = 0; i < bytesRead; ++i)
					{
						if (testBuffer[i] != nextExpectedNumber)
						{
							throw new IOException("Read number " + testBuffer[i] + " but expected " + nextExpectedNumber
								);
						}
						++nextExpectedNumber;
						if (nextExpectedNumber == modulus)
						{
							nextExpectedNumber = 0;
						}
					}
				}
				if (totalBytesRead != expectedSize)
				{
					throw new IOException("Expected to read " + expectedSize + " bytes but only received "
						 + totalBytesRead);
				}
			}
			finally
			{
				inputStream.Close();
			}
		}

		/// <summary>
		/// Generates test data of the given size according to some specific pattern
		/// and writes it to the provided output file.
		/// </summary>
		/// <param name="fs">FileSystem</param>
		/// <param name="path">Test file to be generated</param>
		/// <param name="size">The size of the test data to be generated in bytes</param>
		/// <param name="bufferLen">Pattern length</param>
		/// <param name="modulus">Pattern modulus</param>
		/// <exception cref="System.IO.IOException">thrown if an error occurs while writing the data
		/// 	</exception>
		public static long GenerateTestFile(FileSystem fs, Path path, long size, int bufferLen
			, int modulus)
		{
			byte[] testBuffer = new byte[bufferLen];
			for (int i = 0; i < testBuffer.Length; ++i)
			{
				testBuffer[i] = unchecked((byte)(i % modulus));
			}
			OutputStream outputStream = fs.Create(path, false);
			long bytesWritten = 0;
			try
			{
				while (bytesWritten < size)
				{
					long diff = size - bytesWritten;
					if (diff < testBuffer.Length)
					{
						outputStream.Write(testBuffer, 0, (int)diff);
						bytesWritten += diff;
					}
					else
					{
						outputStream.Write(testBuffer);
						bytesWritten += testBuffer.Length;
					}
				}
				return bytesWritten;
			}
			finally
			{
				outputStream.Close();
			}
		}

		/// <summary>Creates and reads a file with the given size.</summary>
		/// <remarks>
		/// Creates and reads a file with the given size. The test file is generated
		/// according to a specific pattern so it can be easily verified even if it's
		/// a multi-GB one.
		/// During the read phase the incoming data stream is also checked against
		/// this pattern.
		/// </remarks>
		/// <param name="fs">FileSystem</param>
		/// <param name="parent">Test file parent dir path</param>
		/// <exception cref="System.IO.IOException">thrown if an I/O error occurs while writing or reading the test file
		/// 	</exception>
		public static void CreateAndVerifyFile(FileSystem fs, Path parent, long fileSize)
		{
			int testBufferSize = fs.GetConf().GetInt(IoChunkBufferSize, DefaultIoChunkBufferSize
				);
			int modulus = fs.GetConf().GetInt(IoChunkModulusSize, DefaultIoChunkModulusSize);
			string objectName = UUID.RandomUUID().ToString();
			Path objectPath = new Path(parent, objectName);
			// Write test file in a specific pattern
			Assert.Equal(fileSize, GenerateTestFile(fs, objectPath, fileSize
				, testBufferSize, modulus));
			AssertPathExists(fs, "not created successful", objectPath);
			// Now read the same file back and verify its content
			try
			{
				VerifyReceivedData(fs, objectPath, fileSize, testBufferSize, modulus);
			}
			finally
			{
				// Delete test file
				fs.Delete(objectPath, false);
			}
		}
	}
}
