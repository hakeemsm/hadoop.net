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
	public class DefaultCodec : org.apache.hadoop.conf.Configurable, org.apache.hadoop.io.compress.CompressionCodec
		, org.apache.hadoop.io.compress.DirectDecompressionCodec
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.DefaultCodec
			)));

		internal org.apache.hadoop.conf.Configuration conf;

		public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf = conf;
		}

		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			return conf;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.io.compress.CompressionOutputStream createOutputStream
			(java.io.OutputStream @out)
		{
			return org.apache.hadoop.io.compress.CompressionCodec.Util.createOutputStreamWithCodecPool
				(this, conf, @out);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.io.compress.CompressionOutputStream createOutputStream
			(java.io.OutputStream @out, org.apache.hadoop.io.compress.Compressor compressor)
		{
			return new org.apache.hadoop.io.compress.CompressorStream(@out, compressor, conf.
				getInt("io.file.buffer.size", 4 * 1024));
		}

		public virtual java.lang.Class getCompressorType()
		{
			return org.apache.hadoop.io.compress.zlib.ZlibFactory.getZlibCompressorType(conf);
		}

		public virtual org.apache.hadoop.io.compress.Compressor createCompressor()
		{
			return org.apache.hadoop.io.compress.zlib.ZlibFactory.getZlibCompressor(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.io.compress.CompressionInputStream createInputStream
			(java.io.InputStream @in)
		{
			return org.apache.hadoop.io.compress.CompressionCodec.Util.createInputStreamWithCodecPool
				(this, conf, @in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.io.compress.CompressionInputStream createInputStream
			(java.io.InputStream @in, org.apache.hadoop.io.compress.Decompressor decompressor
			)
		{
			return new org.apache.hadoop.io.compress.DecompressorStream(@in, decompressor, conf
				.getInt("io.file.buffer.size", 4 * 1024));
		}

		public virtual java.lang.Class getDecompressorType()
		{
			return org.apache.hadoop.io.compress.zlib.ZlibFactory.getZlibDecompressorType(conf
				);
		}

		public virtual org.apache.hadoop.io.compress.Decompressor createDecompressor()
		{
			return org.apache.hadoop.io.compress.zlib.ZlibFactory.getZlibDecompressor(conf);
		}

		/// <summary><inheritDoc/></summary>
		public virtual org.apache.hadoop.io.compress.DirectDecompressor createDirectDecompressor
			()
		{
			return org.apache.hadoop.io.compress.zlib.ZlibFactory.getZlibDirectDecompressor(conf
				);
		}

		public virtual string getDefaultExtension()
		{
			return ".deflate";
		}
	}
}
