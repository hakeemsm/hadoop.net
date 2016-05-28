/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestFadvisedFileRegion
	{
		private readonly int FileSize = 16 * 1024 * 1024;

		private static readonly Log Log = LogFactory.GetLog(typeof(TestFadvisedFileRegion
			));

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCustomShuffleTransfer()
		{
			FilePath absLogDir = new FilePath("target", typeof(TestFadvisedFileRegion).Name +
				 "LocDir").GetAbsoluteFile();
			string testDirPath = StringUtils.Join(Path.Separator, new string[] { absLogDir.GetAbsolutePath
				(), "testCustomShuffleTransfer" });
			FilePath testDir = new FilePath(testDirPath);
			testDir.Mkdirs();
			System.Console.Out.WriteLine(testDir.GetAbsolutePath());
			FilePath inFile = new FilePath(testDir, "fileIn.out");
			FilePath outFile = new FilePath(testDir, "fileOut.out");
			//Initialize input file
			byte[] initBuff = new byte[FileSize];
			Random rand = new Random();
			rand.NextBytes(initBuff);
			FileOutputStream @out = new FileOutputStream(inFile);
			try
			{
				@out.Write(initBuff);
			}
			finally
			{
				IOUtils.Cleanup(Log, @out);
			}
			//define position and count to read from a file region.
			int position = 2 * 1024 * 1024;
			int count = 4 * 1024 * 1024 - 1;
			RandomAccessFile inputFile = null;
			RandomAccessFile targetFile = null;
			WritableByteChannel target = null;
			FadvisedFileRegion fileRegion = null;
			try
			{
				inputFile = new RandomAccessFile(inFile.GetAbsolutePath(), "r");
				targetFile = new RandomAccessFile(outFile.GetAbsolutePath(), "rw");
				target = targetFile.GetChannel();
				NUnit.Framework.Assert.AreEqual(FileSize, inputFile.Length());
				//create FadvisedFileRegion
				fileRegion = new FadvisedFileRegion(inputFile, position, count, false, 0, null, null
					, 1024, false);
				//test corner cases
				CustomShuffleTransferCornerCases(fileRegion, target, count);
				long pos = 0;
				long size;
				while ((size = fileRegion.CustomShuffleTransfer(target, pos)) > 0)
				{
					pos += size;
				}
				//assert size
				NUnit.Framework.Assert.AreEqual(count, (int)pos);
				NUnit.Framework.Assert.AreEqual(count, targetFile.Length());
			}
			finally
			{
				if (fileRegion != null)
				{
					fileRegion.ReleaseExternalResources();
				}
				IOUtils.Cleanup(Log, target);
				IOUtils.Cleanup(Log, targetFile);
				IOUtils.Cleanup(Log, inputFile);
			}
			//Read the target file and verify that copy is done correctly
			byte[] buff = new byte[FileSize];
			FileInputStream @in = new FileInputStream(outFile);
			try
			{
				int total = @in.Read(buff, 0, count);
				NUnit.Framework.Assert.AreEqual(count, total);
				for (int i = 0; i < count; i++)
				{
					NUnit.Framework.Assert.AreEqual(initBuff[position + i], buff[i]);
				}
			}
			finally
			{
				IOUtils.Cleanup(Log, @in);
			}
			//delete files and folders
			inFile.Delete();
			outFile.Delete();
			testDir.Delete();
			absLogDir.Delete();
		}

		private static void CustomShuffleTransferCornerCases(FadvisedFileRegion fileRegion
			, WritableByteChannel target, int count)
		{
			try
			{
				fileRegion.CustomShuffleTransfer(target, -1);
				NUnit.Framework.Assert.Fail("Expected a IllegalArgumentException");
			}
			catch (ArgumentException)
			{
				Log.Info("Expected - illegal argument is passed.");
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Expected a IllegalArgumentException");
			}
			//test corner cases
			try
			{
				fileRegion.CustomShuffleTransfer(target, count + 1);
				NUnit.Framework.Assert.Fail("Expected a IllegalArgumentException");
			}
			catch (ArgumentException)
			{
				Log.Info("Expected - illegal argument is passed.");
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Expected a IllegalArgumentException");
			}
		}
	}
}
