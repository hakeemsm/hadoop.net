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
using Sharpen;

namespace org.apache.hadoop.io.compress
{
	public class TestCodecFactory : NUnit.Framework.TestCase
	{
		private class BaseCodec : org.apache.hadoop.io.compress.CompressionCodec
		{
			private org.apache.hadoop.conf.Configuration conf;

			public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
			{
				this.conf = conf;
			}

			public virtual org.apache.hadoop.conf.Configuration getConf()
			{
				return conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.io.compress.CompressionOutputStream createOutputStream
				(java.io.OutputStream @out)
			{
				return null;
			}

			public override java.lang.Class getCompressorType()
			{
				return null;
			}

			public override org.apache.hadoop.io.compress.Compressor createCompressor()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.io.compress.CompressionInputStream createInputStream
				(java.io.InputStream @in, org.apache.hadoop.io.compress.Decompressor decompressor
				)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.io.compress.CompressionInputStream createInputStream
				(java.io.InputStream @in)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.io.compress.CompressionOutputStream createOutputStream
				(java.io.OutputStream @out, org.apache.hadoop.io.compress.Compressor compressor)
			{
				return null;
			}

			public override java.lang.Class getDecompressorType()
			{
				return null;
			}

			public override org.apache.hadoop.io.compress.Decompressor createDecompressor()
			{
				return null;
			}

			public override string getDefaultExtension()
			{
				return ".base";
			}
		}

		private class BarCodec : org.apache.hadoop.io.compress.TestCodecFactory.BaseCodec
		{
			public override string getDefaultExtension()
			{
				return "bar";
			}
		}

		private class FooBarCodec : org.apache.hadoop.io.compress.TestCodecFactory.BaseCodec
		{
			public override string getDefaultExtension()
			{
				return ".foo.bar";
			}
		}

		private class FooCodec : org.apache.hadoop.io.compress.TestCodecFactory.BaseCodec
		{
			public override string getDefaultExtension()
			{
				return ".foo";
			}
		}

		private class NewGzipCodec : org.apache.hadoop.io.compress.TestCodecFactory.BaseCodec
		{
			public override string getDefaultExtension()
			{
				return ".gz";
			}
		}

		/// <summary>Returns a factory for a given set of codecs</summary>
		/// <param name="classes">the codec classes to include</param>
		/// <returns>a new factory</returns>
		private static org.apache.hadoop.io.compress.CompressionCodecFactory setClasses(java.lang.Class
			[] classes)
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.io.compress.CompressionCodecFactory.setCodecClasses(conf, java.util.Arrays
				.asList(classes));
			return new org.apache.hadoop.io.compress.CompressionCodecFactory(conf);
		}

		private static void checkCodec(string msg, java.lang.Class expected, org.apache.hadoop.io.compress.CompressionCodec
			 actual)
		{
			NUnit.Framework.Assert.AreEqual(msg + " unexpected codec found", expected.getName
				(), Sharpen.Runtime.getClassForObject(actual).getName());
		}

		public static void testFinding()
		{
			org.apache.hadoop.io.compress.CompressionCodecFactory factory = new org.apache.hadoop.io.compress.CompressionCodecFactory
				(new org.apache.hadoop.conf.Configuration());
			org.apache.hadoop.io.compress.CompressionCodec codec = factory.getCodec(new org.apache.hadoop.fs.Path
				("/tmp/foo.bar"));
			NUnit.Framework.Assert.AreEqual("default factory foo codec", null, codec);
			codec = factory.getCodecByClassName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.BarCodec
				)).getCanonicalName());
			NUnit.Framework.Assert.AreEqual("default factory foo codec", null, codec);
			codec = factory.getCodec(new org.apache.hadoop.fs.Path("/tmp/foo.gz"));
			checkCodec("default factory for .gz", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.GzipCodec
				)), codec);
			codec = factory.getCodecByClassName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.GzipCodec
				)).getCanonicalName());
			checkCodec("default factory for gzip codec", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.GzipCodec)), codec);
			codec = factory.getCodecByName("gzip");
			checkCodec("default factory for gzip codec", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.GzipCodec)), codec);
			codec = factory.getCodecByName("GZIP");
			checkCodec("default factory for gzip codec", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.GzipCodec)), codec);
			codec = factory.getCodecByName("GZIPCodec");
			checkCodec("default factory for gzip codec", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.GzipCodec)), codec);
			codec = factory.getCodecByName("gzipcodec");
			checkCodec("default factory for gzip codec", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.GzipCodec)), codec);
			java.lang.Class klass = factory.getCodecClassByName("gzipcodec");
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.GzipCodec
				)), klass);
			codec = factory.getCodec(new org.apache.hadoop.fs.Path("/tmp/foo.bz2"));
			checkCodec("default factory for .bz2", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.BZip2Codec
				)), codec);
			codec = factory.getCodecByClassName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.BZip2Codec
				)).getCanonicalName());
			checkCodec("default factory for bzip2 codec", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.BZip2Codec)), codec);
			codec = factory.getCodecByName("bzip2");
			checkCodec("default factory for bzip2 codec", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.BZip2Codec)), codec);
			codec = factory.getCodecByName("bzip2codec");
			checkCodec("default factory for bzip2 codec", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.BZip2Codec)), codec);
			codec = factory.getCodecByName("BZIP2");
			checkCodec("default factory for bzip2 codec", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.BZip2Codec)), codec);
			codec = factory.getCodecByName("BZIP2CODEC");
			checkCodec("default factory for bzip2 codec", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.BZip2Codec)), codec);
			codec = factory.getCodecByClassName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.DeflateCodec
				)).getCanonicalName());
			checkCodec("default factory for deflate codec", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.DeflateCodec)), codec);
			codec = factory.getCodecByName("deflate");
			checkCodec("default factory for deflate codec", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.DeflateCodec)), codec);
			codec = factory.getCodecByName("deflatecodec");
			checkCodec("default factory for deflate codec", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.DeflateCodec)), codec);
			codec = factory.getCodecByName("DEFLATE");
			checkCodec("default factory for deflate codec", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.DeflateCodec)), codec);
			codec = factory.getCodecByName("DEFLATECODEC");
			checkCodec("default factory for deflate codec", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.DeflateCodec)), codec);
			factory = setClasses(new java.lang.Class[0]);
			// gz, bz2, snappy, lz4 are picked up by service loader, but bar isn't
			codec = factory.getCodec(new org.apache.hadoop.fs.Path("/tmp/foo.bar"));
			NUnit.Framework.Assert.AreEqual("empty factory bar codec", null, codec);
			codec = factory.getCodecByClassName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.BarCodec
				)).getCanonicalName());
			NUnit.Framework.Assert.AreEqual("empty factory bar codec", null, codec);
			codec = factory.getCodec(new org.apache.hadoop.fs.Path("/tmp/foo.gz"));
			checkCodec("empty factory gz codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.GzipCodec
				)), codec);
			codec = factory.getCodecByClassName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.GzipCodec
				)).getCanonicalName());
			checkCodec("empty factory gz codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.GzipCodec
				)), codec);
			codec = factory.getCodec(new org.apache.hadoop.fs.Path("/tmp/foo.bz2"));
			checkCodec("empty factory for .bz2", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.BZip2Codec
				)), codec);
			codec = factory.getCodecByClassName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.BZip2Codec
				)).getCanonicalName());
			checkCodec("empty factory for bzip2 codec", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.BZip2Codec)), codec);
			codec = factory.getCodec(new org.apache.hadoop.fs.Path("/tmp/foo.snappy"));
			checkCodec("empty factory snappy codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.SnappyCodec
				)), codec);
			codec = factory.getCodecByClassName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.SnappyCodec
				)).getCanonicalName());
			checkCodec("empty factory snappy codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.SnappyCodec
				)), codec);
			codec = factory.getCodec(new org.apache.hadoop.fs.Path("/tmp/foo.lz4"));
			checkCodec("empty factory lz4 codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.Lz4Codec
				)), codec);
			codec = factory.getCodecByClassName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.Lz4Codec
				)).getCanonicalName());
			checkCodec("empty factory lz4 codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.Lz4Codec
				)), codec);
			factory = setClasses(new java.lang.Class[] { Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.TestCodecFactory.BarCodec)), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.compress.TestCodecFactory.FooCodec)), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.compress.TestCodecFactory.FooBarCodec)) });
			codec = factory.getCodec(new org.apache.hadoop.fs.Path("/tmp/.foo.bar.gz"));
			checkCodec("full factory gz codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.GzipCodec
				)), codec);
			codec = factory.getCodecByClassName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.GzipCodec
				)).getCanonicalName());
			checkCodec("full codec gz codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.GzipCodec
				)), codec);
			codec = factory.getCodec(new org.apache.hadoop.fs.Path("/tmp/foo.bz2"));
			checkCodec("full factory for .bz2", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.BZip2Codec
				)), codec);
			codec = factory.getCodecByClassName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.BZip2Codec
				)).getCanonicalName());
			checkCodec("full codec bzip2 codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.BZip2Codec
				)), codec);
			codec = factory.getCodec(new org.apache.hadoop.fs.Path("/tmp/foo.bar"));
			checkCodec("full factory bar codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.BarCodec
				)), codec);
			codec = factory.getCodecByClassName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.BarCodec
				)).getCanonicalName());
			checkCodec("full factory bar codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.BarCodec
				)), codec);
			codec = factory.getCodecByName("bar");
			checkCodec("full factory bar codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.BarCodec
				)), codec);
			codec = factory.getCodecByName("BAR");
			checkCodec("full factory bar codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.BarCodec
				)), codec);
			codec = factory.getCodec(new org.apache.hadoop.fs.Path("/tmp/foo/baz.foo.bar"));
			checkCodec("full factory foo bar codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.FooBarCodec
				)), codec);
			codec = factory.getCodecByClassName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.FooBarCodec
				)).getCanonicalName());
			checkCodec("full factory foo bar codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.FooBarCodec
				)), codec);
			codec = factory.getCodecByName("foobar");
			checkCodec("full factory foo bar codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.FooBarCodec
				)), codec);
			codec = factory.getCodecByName("FOOBAR");
			checkCodec("full factory foo bar codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.FooBarCodec
				)), codec);
			codec = factory.getCodec(new org.apache.hadoop.fs.Path("/tmp/foo.foo"));
			checkCodec("full factory foo codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.FooCodec
				)), codec);
			codec = factory.getCodecByClassName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.FooCodec
				)).getCanonicalName());
			checkCodec("full factory foo codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.FooCodec
				)), codec);
			codec = factory.getCodecByName("foo");
			checkCodec("full factory foo codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.FooCodec
				)), codec);
			codec = factory.getCodecByName("FOO");
			checkCodec("full factory foo codec", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.FooCodec
				)), codec);
			factory = setClasses(new java.lang.Class[] { Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.TestCodecFactory.NewGzipCodec)) });
			codec = factory.getCodec(new org.apache.hadoop.fs.Path("/tmp/foo.gz"));
			checkCodec("overridden factory for .gz", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.NewGzipCodec
				)), codec);
			codec = factory.getCodecByClassName(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.TestCodecFactory.NewGzipCodec
				)).getCanonicalName());
			checkCodec("overridden factory for gzip codec", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.compress.TestCodecFactory.NewGzipCodec)), codec);
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set(org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODECS_KEY, 
				"   org.apache.hadoop.io.compress.GzipCodec   , " + "    org.apache.hadoop.io.compress.DefaultCodec  , "
				 + " org.apache.hadoop.io.compress.BZip2Codec   ");
			try
			{
				org.apache.hadoop.io.compress.CompressionCodecFactory.getCodecClasses(conf);
			}
			catch (System.ArgumentException)
			{
				fail("IllegalArgumentException is unexpected");
			}
		}
	}
}
