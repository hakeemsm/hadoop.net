using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Math3.Util;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestGenericOptionsParser : TestCase
	{
		internal FilePath testDir;

		internal Configuration conf;

		internal FileSystem localFs;

		/// <exception cref="System.Exception"/>
		public virtual void TestFilesOption()
		{
			FilePath tmpFile = new FilePath(testDir, "tmpfile");
			Path tmpPath = new Path(tmpFile.ToString());
			localFs.Create(tmpPath);
			string[] args = new string[2];
			// pass a files option 
			args[0] = "-files";
			// Convert a file to a URI as File.toString() is not a valid URI on
			// all platforms and GenericOptionsParser accepts only valid URIs
			args[1] = tmpFile.ToURI().ToString();
			new GenericOptionsParser(conf, args);
			string files = conf.Get("tmpfiles");
			NUnit.Framework.Assert.IsNotNull("files is null", files);
			Assert.Equal("files option does not match", localFs.MakeQualified
				(tmpPath).ToString(), files);
			// pass file as uri
			Configuration conf1 = new Configuration();
			URI tmpURI = new URI(tmpFile.ToURI().ToString() + "#link");
			args[0] = "-files";
			args[1] = tmpURI.ToString();
			new GenericOptionsParser(conf1, args);
			files = conf1.Get("tmpfiles");
			NUnit.Framework.Assert.IsNotNull("files is null", files);
			Assert.Equal("files option does not match", localFs.MakeQualified
				(new Path(tmpURI)).ToString(), files);
			// pass a file that does not exist.
			// GenericOptionParser should throw exception
			Configuration conf2 = new Configuration();
			args[0] = "-files";
			args[1] = "file:///xyz.txt";
			Exception th = null;
			try
			{
				new GenericOptionsParser(conf2, args);
			}
			catch (Exception e)
			{
				th = e;
			}
			NUnit.Framework.Assert.IsNotNull("throwable is null", th);
			Assert.True("FileNotFoundException is not thrown", th is FileNotFoundException
				);
			files = conf2.Get("tmpfiles");
			NUnit.Framework.Assert.IsNull("files is not null", files);
		}

		/// <summary>
		/// Test the case where the libjars, files and archives arguments
		/// contains an empty token, which should create an IllegalArgumentException.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestEmptyFilenames()
		{
			IList<Pair<string, string>> argsAndConfNames = new AList<Pair<string, string>>();
			argsAndConfNames.AddItem(new Pair<string, string>("-libjars", "tmpjars"));
			argsAndConfNames.AddItem(new Pair<string, string>("-files", "tmpfiles"));
			argsAndConfNames.AddItem(new Pair<string, string>("-archives", "tmparchives"));
			foreach (Pair<string, string> argAndConfName in argsAndConfNames)
			{
				string arg = argAndConfName.GetFirst();
				string configName = argAndConfName.GetSecond();
				FilePath tmpFileOne = new FilePath(testDir, "tmpfile1");
				Path tmpPathOne = new Path(tmpFileOne.ToString());
				FilePath tmpFileTwo = new FilePath(testDir, "tmpfile2");
				Path tmpPathTwo = new Path(tmpFileTwo.ToString());
				localFs.Create(tmpPathOne);
				localFs.Create(tmpPathTwo);
				string[] args = new string[2];
				args[0] = arg;
				// create an empty path in between two valid files,
				// which prior to HADOOP-10820 used to result in the
				// working directory being added to "tmpjars" (or equivalent)
				args[1] = string.Format("%s,,%s", tmpFileOne.ToURI().ToString(), tmpFileTwo.ToURI
					().ToString());
				try
				{
					new GenericOptionsParser(conf, args);
					Fail("Expected exception for empty filename");
				}
				catch (ArgumentException e)
				{
					// expect to receive an IllegalArgumentException
					GenericTestUtils.AssertExceptionContains("File name can't be" + " empty string", 
						e);
				}
				// test zero file list length - it should create an exception
				args[1] = ",,";
				try
				{
					new GenericOptionsParser(conf, args);
					Fail("Expected exception for zero file list length");
				}
				catch (ArgumentException e)
				{
					// expect to receive an IllegalArgumentException
					GenericTestUtils.AssertExceptionContains("File name can't be" + " empty string", 
						e);
				}
				// test filename with space character
				// it should create exception from parser in URI class
				// due to URI syntax error
				args[1] = string.Format("%s, ,%s", tmpFileOne.ToURI().ToString(), tmpFileTwo.ToURI
					().ToString());
				try
				{
					new GenericOptionsParser(conf, args);
					Fail("Expected exception for filename with space character");
				}
				catch (ArgumentException e)
				{
					// expect to receive an IllegalArgumentException
					GenericTestUtils.AssertExceptionContains("URISyntaxException", e);
				}
			}
		}

		/// <summary>Test that options passed to the constructor are used.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCreateWithOptions()
		{
			// Create new option newOpt
			Option opt = OptionBuilder.Create("newOpt");
			Options opts = new Options();
			opts.AddOption(opt);
			// Check newOpt is actually used to parse the args
			string[] args = new string[2];
			args[0] = "--newOpt";
			args[1] = "7";
			GenericOptionsParser g = new GenericOptionsParser(opts, args);
			Assert.Equal("New option was ignored", "7", g.GetCommandLine()
				.GetOptionValues("newOpt")[0]);
		}

		/// <summary>Test that multiple conf arguments can be used.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestConfWithMultipleOpts()
		{
			string[] args = new string[2];
			args[0] = "--conf=foo";
			args[1] = "--conf=bar";
			GenericOptionsParser g = new GenericOptionsParser(args);
			Assert.Equal("1st conf param is incorrect", "foo", g.GetCommandLine
				().GetOptionValues("conf")[0]);
			Assert.Equal("2st conf param is incorrect", "bar", g.GetCommandLine
				().GetOptionValues("conf")[1]);
		}

		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			base.SetUp();
			conf = new Configuration();
			localFs = FileSystem.GetLocal(conf);
			testDir = new FilePath(Runtime.GetProperty("test.build.data", "/tmp"), "generic");
			if (testDir.Exists())
			{
				localFs.Delete(new Path(testDir.ToString()), true);
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			base.TearDown();
			if (testDir.Exists())
			{
				localFs.Delete(new Path(testDir.ToString()), true);
			}
		}

		/// <summary>testing -fileCache option</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestTokenCacheOption()
		{
			FileSystem localFs = FileSystem.GetLocal(conf);
			FilePath tmpFile = new FilePath(testDir, "tokenCacheFile");
			if (tmpFile.Exists())
			{
				tmpFile.Delete();
			}
			string[] args = new string[2];
			// pass a files option 
			args[0] = "-tokenCacheFile";
			args[1] = tmpFile.ToURI().ToString();
			// test non existing file
			Exception th = null;
			try
			{
				new GenericOptionsParser(conf, args);
			}
			catch (Exception e)
			{
				th = e;
			}
			NUnit.Framework.Assert.IsNotNull(th);
			Assert.True("FileNotFoundException is not thrown", th is FileNotFoundException
				);
			// create file
			Path tmpPath = localFs.MakeQualified(new Path(tmpFile.ToString()));
			Org.Apache.Hadoop.Security.Token.Token<object> token = new Org.Apache.Hadoop.Security.Token.Token
				<AbstractDelegationTokenIdentifier>(Sharpen.Runtime.GetBytesForString("identifier"
				), Sharpen.Runtime.GetBytesForString("password"), new Text("token-kind"), new Text
				("token-service"));
			Credentials creds = new Credentials();
			creds.AddToken(new Text("token-alias"), token);
			creds.WriteTokenStorageFile(tmpPath, conf);
			new GenericOptionsParser(conf, args);
			string fileName = conf.Get("mapreduce.job.credentials.binary");
			NUnit.Framework.Assert.IsNotNull("files is null", fileName);
			Assert.Equal("files option does not match", tmpPath.ToString()
				, fileName);
			Credentials ugiCreds = UserGroupInformation.GetCurrentUser().GetCredentials();
			Assert.Equal(1, ugiCreds.NumberOfTokens());
			Org.Apache.Hadoop.Security.Token.Token<object> ugiToken = ugiCreds.GetToken(new Text
				("token-alias"));
			NUnit.Framework.Assert.IsNotNull(ugiToken);
			Assert.Equal(token, ugiToken);
			localFs.Delete(new Path(testDir.GetAbsolutePath()), true);
		}

		/// <summary>Test -D parsing</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDOptionParsing()
		{
			string[] args;
			IDictionary<string, string> expectedMap;
			string[] expectedRemainingArgs;
			args = new string[] {  };
			expectedRemainingArgs = new string[] {  };
			expectedMap = Maps.NewHashMap();
			AssertDOptionParsing(args, expectedMap, expectedRemainingArgs);
			args = new string[] { "-Dkey1=value1" };
			expectedRemainingArgs = new string[] {  };
			expectedMap = Maps.NewHashMap();
			expectedMap["key1"] = "value1";
			AssertDOptionParsing(args, expectedMap, expectedRemainingArgs);
			args = new string[] { "-fs", "hdfs://somefs/", "-Dkey1=value1", "arg1" };
			expectedRemainingArgs = new string[] { "arg1" };
			AssertDOptionParsing(args, expectedMap, expectedRemainingArgs);
			args = new string[] { "-fs", "hdfs://somefs/", "-D", "key1=value1", "arg1" };
			AssertDOptionParsing(args, expectedMap, expectedRemainingArgs);
			if (Shell.Windows)
			{
				args = new string[] { "-fs", "hdfs://somefs/", "-D", "key1", "value1", "arg1" };
				AssertDOptionParsing(args, expectedMap, expectedRemainingArgs);
				args = new string[] { "-fs", "hdfs://somefs/", "-Dkey1", "value1", "arg1" };
				AssertDOptionParsing(args, expectedMap, expectedRemainingArgs);
				args = new string[] { "-fs", "hdfs://somefs/", "-D", "key1", "value1", "-fs", "someother"
					, "-D", "key2", "value2", "arg1", "arg2" };
				expectedRemainingArgs = new string[] { "arg1", "arg2" };
				expectedMap = Maps.NewHashMap();
				expectedMap["key1"] = "value1";
				expectedMap["key2"] = "value2";
				AssertDOptionParsing(args, expectedMap, expectedRemainingArgs);
				args = new string[] { "-fs", "hdfs://somefs/", "-D", "key1", "value1", "-fs", "someother"
					, "-D", "key2", "value2" };
				expectedRemainingArgs = new string[] {  };
				AssertDOptionParsing(args, expectedMap, expectedRemainingArgs);
				args = new string[] { "-fs", "hdfs://somefs/", "-D", "key1", "value1", "-fs", "someother"
					, "-D", "key2" };
				expectedMap = Maps.NewHashMap();
				expectedMap["key1"] = "value1";
				expectedMap["key2"] = null;
				// we expect key2 not set
				AssertDOptionParsing(args, expectedMap, expectedRemainingArgs);
			}
			args = new string[] { "-fs", "hdfs://somefs/", "-D", "key1=value1", "-fs", "someother"
				, "-Dkey2" };
			expectedRemainingArgs = new string[] {  };
			expectedMap = Maps.NewHashMap();
			expectedMap["key1"] = "value1";
			expectedMap["key2"] = null;
			// we expect key2 not set
			AssertDOptionParsing(args, expectedMap, expectedRemainingArgs);
			args = new string[] { "-fs", "hdfs://somefs/", "-D" };
			expectedMap = Maps.NewHashMap();
			AssertDOptionParsing(args, expectedMap, expectedRemainingArgs);
		}

		/// <exception cref="System.Exception"/>
		private void AssertDOptionParsing(string[] args, IDictionary<string, string> expectedMap
			, string[] expectedRemainingArgs)
		{
			foreach (KeyValuePair<string, string> entry in expectedMap)
			{
				NUnit.Framework.Assert.IsNull(conf.Get(entry.Key));
			}
			Configuration conf = new Configuration();
			GenericOptionsParser parser = new GenericOptionsParser(conf, args);
			string[] remainingArgs = parser.GetRemainingArgs();
			foreach (KeyValuePair<string, string> entry_1 in expectedMap)
			{
				Assert.Equal(entry_1.Value, conf.Get(entry_1.Key));
			}
			Assert.AssertArrayEquals(Arrays.ToString(remainingArgs) + Arrays.ToString(expectedRemainingArgs
				), expectedRemainingArgs, remainingArgs);
		}

		/// <summary>Test passing null as args.</summary>
		/// <remarks>
		/// Test passing null as args. Some classes still call
		/// Tool interface from java passing null.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNullArgs()
		{
			GenericOptionsParser parser = new GenericOptionsParser(conf, null);
			parser.GetRemainingArgs();
		}
	}
}
