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
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress
{
	public class TestCodecFactory : TestCase
	{
		private class BaseCodec : CompressionCodec
		{
			private Configuration conf;

			public virtual void SetConf(Configuration conf)
			{
				this.conf = conf;
			}

			public virtual Configuration GetConf()
			{
				return conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public override CompressionOutputStream CreateOutputStream(OutputStream @out)
			{
				return null;
			}

			public override Type GetCompressorType()
			{
				return null;
			}

			public override Compressor CreateCompressor()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override CompressionInputStream CreateInputStream(InputStream @in, Decompressor
				 decompressor)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override CompressionInputStream CreateInputStream(InputStream @in)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override CompressionOutputStream CreateOutputStream(OutputStream @out, Compressor
				 compressor)
			{
				return null;
			}

			public override Type GetDecompressorType()
			{
				return null;
			}

			public override Decompressor CreateDecompressor()
			{
				return null;
			}

			public override string GetDefaultExtension()
			{
				return ".base";
			}
		}

		private class BarCodec : TestCodecFactory.BaseCodec
		{
			public override string GetDefaultExtension()
			{
				return "bar";
			}
		}

		private class FooBarCodec : TestCodecFactory.BaseCodec
		{
			public override string GetDefaultExtension()
			{
				return ".foo.bar";
			}
		}

		private class FooCodec : TestCodecFactory.BaseCodec
		{
			public override string GetDefaultExtension()
			{
				return ".foo";
			}
		}

		private class NewGzipCodec : TestCodecFactory.BaseCodec
		{
			public override string GetDefaultExtension()
			{
				return ".gz";
			}
		}

		/// <summary>Returns a factory for a given set of codecs</summary>
		/// <param name="classes">the codec classes to include</param>
		/// <returns>a new factory</returns>
		private static CompressionCodecFactory SetClasses(Type[] classes)
		{
			Configuration conf = new Configuration();
			CompressionCodecFactory.SetCodecClasses(conf, Arrays.AsList(classes));
			return new CompressionCodecFactory(conf);
		}

		private static void CheckCodec(string msg, Type expected, CompressionCodec actual
			)
		{
			NUnit.Framework.Assert.AreEqual(msg + " unexpected codec found", expected.FullName
				, actual.GetType().FullName);
		}

		public static void TestFinding()
		{
			CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration()
				);
			CompressionCodec codec = factory.GetCodec(new Path("/tmp/foo.bar"));
			NUnit.Framework.Assert.AreEqual("default factory foo codec", null, codec);
			codec = factory.GetCodecByClassName(typeof(TestCodecFactory.BarCodec).GetCanonicalName
				());
			NUnit.Framework.Assert.AreEqual("default factory foo codec", null, codec);
			codec = factory.GetCodec(new Path("/tmp/foo.gz"));
			CheckCodec("default factory for .gz", typeof(GzipCodec), codec);
			codec = factory.GetCodecByClassName(typeof(GzipCodec).GetCanonicalName());
			CheckCodec("default factory for gzip codec", typeof(GzipCodec), codec);
			codec = factory.GetCodecByName("gzip");
			CheckCodec("default factory for gzip codec", typeof(GzipCodec), codec);
			codec = factory.GetCodecByName("GZIP");
			CheckCodec("default factory for gzip codec", typeof(GzipCodec), codec);
			codec = factory.GetCodecByName("GZIPCodec");
			CheckCodec("default factory for gzip codec", typeof(GzipCodec), codec);
			codec = factory.GetCodecByName("gzipcodec");
			CheckCodec("default factory for gzip codec", typeof(GzipCodec), codec);
			Type klass = factory.GetCodecClassByName("gzipcodec");
			NUnit.Framework.Assert.AreEqual(typeof(GzipCodec), klass);
			codec = factory.GetCodec(new Path("/tmp/foo.bz2"));
			CheckCodec("default factory for .bz2", typeof(BZip2Codec), codec);
			codec = factory.GetCodecByClassName(typeof(BZip2Codec).GetCanonicalName());
			CheckCodec("default factory for bzip2 codec", typeof(BZip2Codec), codec);
			codec = factory.GetCodecByName("bzip2");
			CheckCodec("default factory for bzip2 codec", typeof(BZip2Codec), codec);
			codec = factory.GetCodecByName("bzip2codec");
			CheckCodec("default factory for bzip2 codec", typeof(BZip2Codec), codec);
			codec = factory.GetCodecByName("BZIP2");
			CheckCodec("default factory for bzip2 codec", typeof(BZip2Codec), codec);
			codec = factory.GetCodecByName("BZIP2CODEC");
			CheckCodec("default factory for bzip2 codec", typeof(BZip2Codec), codec);
			codec = factory.GetCodecByClassName(typeof(DeflateCodec).GetCanonicalName());
			CheckCodec("default factory for deflate codec", typeof(DeflateCodec), codec);
			codec = factory.GetCodecByName("deflate");
			CheckCodec("default factory for deflate codec", typeof(DeflateCodec), codec);
			codec = factory.GetCodecByName("deflatecodec");
			CheckCodec("default factory for deflate codec", typeof(DeflateCodec), codec);
			codec = factory.GetCodecByName("DEFLATE");
			CheckCodec("default factory for deflate codec", typeof(DeflateCodec), codec);
			codec = factory.GetCodecByName("DEFLATECODEC");
			CheckCodec("default factory for deflate codec", typeof(DeflateCodec), codec);
			factory = SetClasses(new Type[0]);
			// gz, bz2, snappy, lz4 are picked up by service loader, but bar isn't
			codec = factory.GetCodec(new Path("/tmp/foo.bar"));
			NUnit.Framework.Assert.AreEqual("empty factory bar codec", null, codec);
			codec = factory.GetCodecByClassName(typeof(TestCodecFactory.BarCodec).GetCanonicalName
				());
			NUnit.Framework.Assert.AreEqual("empty factory bar codec", null, codec);
			codec = factory.GetCodec(new Path("/tmp/foo.gz"));
			CheckCodec("empty factory gz codec", typeof(GzipCodec), codec);
			codec = factory.GetCodecByClassName(typeof(GzipCodec).GetCanonicalName());
			CheckCodec("empty factory gz codec", typeof(GzipCodec), codec);
			codec = factory.GetCodec(new Path("/tmp/foo.bz2"));
			CheckCodec("empty factory for .bz2", typeof(BZip2Codec), codec);
			codec = factory.GetCodecByClassName(typeof(BZip2Codec).GetCanonicalName());
			CheckCodec("empty factory for bzip2 codec", typeof(BZip2Codec), codec);
			codec = factory.GetCodec(new Path("/tmp/foo.snappy"));
			CheckCodec("empty factory snappy codec", typeof(SnappyCodec), codec);
			codec = factory.GetCodecByClassName(typeof(SnappyCodec).GetCanonicalName());
			CheckCodec("empty factory snappy codec", typeof(SnappyCodec), codec);
			codec = factory.GetCodec(new Path("/tmp/foo.lz4"));
			CheckCodec("empty factory lz4 codec", typeof(Lz4Codec), codec);
			codec = factory.GetCodecByClassName(typeof(Lz4Codec).GetCanonicalName());
			CheckCodec("empty factory lz4 codec", typeof(Lz4Codec), codec);
			factory = SetClasses(new Type[] { typeof(TestCodecFactory.BarCodec), typeof(TestCodecFactory.FooCodec
				), typeof(TestCodecFactory.FooBarCodec) });
			codec = factory.GetCodec(new Path("/tmp/.foo.bar.gz"));
			CheckCodec("full factory gz codec", typeof(GzipCodec), codec);
			codec = factory.GetCodecByClassName(typeof(GzipCodec).GetCanonicalName());
			CheckCodec("full codec gz codec", typeof(GzipCodec), codec);
			codec = factory.GetCodec(new Path("/tmp/foo.bz2"));
			CheckCodec("full factory for .bz2", typeof(BZip2Codec), codec);
			codec = factory.GetCodecByClassName(typeof(BZip2Codec).GetCanonicalName());
			CheckCodec("full codec bzip2 codec", typeof(BZip2Codec), codec);
			codec = factory.GetCodec(new Path("/tmp/foo.bar"));
			CheckCodec("full factory bar codec", typeof(TestCodecFactory.BarCodec), codec);
			codec = factory.GetCodecByClassName(typeof(TestCodecFactory.BarCodec).GetCanonicalName
				());
			CheckCodec("full factory bar codec", typeof(TestCodecFactory.BarCodec), codec);
			codec = factory.GetCodecByName("bar");
			CheckCodec("full factory bar codec", typeof(TestCodecFactory.BarCodec), codec);
			codec = factory.GetCodecByName("BAR");
			CheckCodec("full factory bar codec", typeof(TestCodecFactory.BarCodec), codec);
			codec = factory.GetCodec(new Path("/tmp/foo/baz.foo.bar"));
			CheckCodec("full factory foo bar codec", typeof(TestCodecFactory.FooBarCodec), codec
				);
			codec = factory.GetCodecByClassName(typeof(TestCodecFactory.FooBarCodec).GetCanonicalName
				());
			CheckCodec("full factory foo bar codec", typeof(TestCodecFactory.FooBarCodec), codec
				);
			codec = factory.GetCodecByName("foobar");
			CheckCodec("full factory foo bar codec", typeof(TestCodecFactory.FooBarCodec), codec
				);
			codec = factory.GetCodecByName("FOOBAR");
			CheckCodec("full factory foo bar codec", typeof(TestCodecFactory.FooBarCodec), codec
				);
			codec = factory.GetCodec(new Path("/tmp/foo.foo"));
			CheckCodec("full factory foo codec", typeof(TestCodecFactory.FooCodec), codec);
			codec = factory.GetCodecByClassName(typeof(TestCodecFactory.FooCodec).GetCanonicalName
				());
			CheckCodec("full factory foo codec", typeof(TestCodecFactory.FooCodec), codec);
			codec = factory.GetCodecByName("foo");
			CheckCodec("full factory foo codec", typeof(TestCodecFactory.FooCodec), codec);
			codec = factory.GetCodecByName("FOO");
			CheckCodec("full factory foo codec", typeof(TestCodecFactory.FooCodec), codec);
			factory = SetClasses(new Type[] { typeof(TestCodecFactory.NewGzipCodec) });
			codec = factory.GetCodec(new Path("/tmp/foo.gz"));
			CheckCodec("overridden factory for .gz", typeof(TestCodecFactory.NewGzipCodec), codec
				);
			codec = factory.GetCodecByClassName(typeof(TestCodecFactory.NewGzipCodec).GetCanonicalName
				());
			CheckCodec("overridden factory for gzip codec", typeof(TestCodecFactory.NewGzipCodec
				), codec);
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeys.IoCompressionCodecsKey, "   org.apache.hadoop.io.compress.GzipCodec   , "
				 + "    org.apache.hadoop.io.compress.DefaultCodec  , " + " org.apache.hadoop.io.compress.BZip2Codec   "
				);
			try
			{
				CompressionCodecFactory.GetCodecClasses(conf);
			}
			catch (ArgumentException)
			{
				Fail("IllegalArgumentException is unexpected");
			}
		}
	}
}
