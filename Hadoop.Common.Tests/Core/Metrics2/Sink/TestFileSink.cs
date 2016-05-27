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
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Impl;
using Org.Apache.Hadoop.Metrics2.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Sink
{
	public class TestFileSink
	{
		private FilePath outFile;

		internal class MyMetrics1
		{
			// The 2 sample metric classes:
			internal virtual string TestTag1()
			{
				return "testTagValue1";
			}

			internal virtual string GettestTag2()
			{
				return "testTagValue2";
			}

			internal MutableGaugeInt testMetric1;

			internal MutableGaugeInt testMetric2;

			public virtual TestFileSink.MyMetrics1 RegisterWith(MetricsSystem ms)
			{
				return ms.Register("m1", null, this);
			}
		}

		internal class MyMetrics2
		{
			internal virtual string TestTag1()
			{
				return "testTagValue22";
			}

			public virtual TestFileSink.MyMetrics2 RegisterWith(MetricsSystem ms)
			{
				return ms.Register("m2", null, this);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private FilePath GetTestTempFile(string prefix, string suffix)
		{
			string tmpPath = Runtime.GetProperty("java.io.tmpdir", "/tmp");
			string user = Runtime.GetProperty("user.name", "unknown-user");
			FilePath dir = new FilePath(tmpPath + "/" + user);
			dir.Mkdirs();
			return FilePath.CreateTempFile(prefix, suffix, dir);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFileSink()
		{
			outFile = GetTestTempFile("test-file-sink-", ".out");
			string outPath = outFile.GetAbsolutePath();
			// NB: specify large period to avoid multiple metrics snapshotting: 
			new ConfigBuilder().Add("*.period", 10000).Add("test.sink.mysink0.class", typeof(
				FileSink).FullName).Add("test.sink.mysink0.filename", outPath).Add("test.sink.mysink0.context"
				, "test1").Save(TestMetricsConfig.GetTestFilename("hadoop-metrics2-test"));
			// NB: we filter by context to exclude "metricssystem" context metrics:
			MetricsSystemImpl ms = new MetricsSystemImpl("test");
			ms.Start();
			TestFileSink.MyMetrics1 mm1 = new TestFileSink.MyMetrics1().RegisterWith(ms);
			new TestFileSink.MyMetrics2().RegisterWith(ms);
			mm1.testMetric1.Incr();
			mm1.testMetric2.Incr(2);
			ms.PublishMetricsNow();
			// publish the metrics
			ms.Stop();
			ms.Shutdown();
			InputStream @is = null;
			ByteArrayOutputStream baos = null;
			string outFileContent = null;
			try
			{
				@is = new FileInputStream(outFile);
				baos = new ByteArrayOutputStream((int)outFile.Length());
				IOUtils.CopyBytes(@is, baos, 1024, true);
				outFileContent = Sharpen.Runtime.GetStringForBytes(baos.ToByteArray(), "UTF-8");
			}
			finally
			{
				IOUtils.Cleanup(null, baos, @is);
			}
			// Check the out file content. Should be something like the following:
			//1360244820087 test1.testRecord1: Context=test1, testTag1=testTagValue1, testTag2=testTagValue2, Hostname=myhost, testMetric1=1, testMetric2=2
			//1360244820089 test1.testRecord2: Context=test1, testTag22=testTagValue22, Hostname=myhost
			// Note that in the below expression we allow tags and metrics to go in arbitrary order.  
			Sharpen.Pattern expectedContentPattern = Sharpen.Pattern.Compile("^\\d+\\s+test1.testRecord1:\\s+Context=test1,\\s+"
				 + "(testTag1=testTagValue1,\\s+testTag2=testTagValue2|testTag2=testTagValue2,\\s+testTag1=testTagValue1),"
				 + "\\s+Hostname=.*,\\s+(testMetric1=1,\\s+testMetric2=2|testMetric2=2,\\s+testMetric1=1)"
				 + "$[\\n\\r]*^\\d+\\s+test1.testRecord2:\\s+Context=test1," + "\\s+testTag22=testTagValue22,\\s+Hostname=.*$[\\n\\r]*"
				, Sharpen.Pattern.Multiline);
			// line #1:
			// line #2:
			NUnit.Framework.Assert.IsTrue(expectedContentPattern.Matcher(outFileContent).Matches
				());
		}

		[TearDown]
		public virtual void After()
		{
			if (outFile != null)
			{
				outFile.Delete();
				NUnit.Framework.Assert.IsTrue(!outFile.Exists());
			}
		}
	}
}
