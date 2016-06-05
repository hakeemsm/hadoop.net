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
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO.Compress.Zlib;


namespace Org.Apache.Hadoop.IO.Compress
{
	public class DefaultCodec : Configurable, CompressionCodec, DirectDecompressionCodec
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(DefaultCodec));

		internal Configuration conf;

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual CompressionOutputStream CreateOutputStream(OutputStream @out)
		{
			return CompressionCodec.Util.CreateOutputStreamWithCodecPool(this, conf, @out);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual CompressionOutputStream CreateOutputStream(OutputStream @out, Compressor
			 compressor)
		{
			return new CompressorStream(@out, compressor, conf.GetInt("io.file.buffer.size", 
				4 * 1024));
		}

		public virtual Type GetCompressorType()
		{
			return ZlibFactory.GetZlibCompressorType(conf);
		}

		public virtual Compressor CreateCompressor()
		{
			return ZlibFactory.GetZlibCompressor(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual CompressionInputStream CreateInputStream(InputStream @in)
		{
			return CompressionCodec.Util.CreateInputStreamWithCodecPool(this, conf, @in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual CompressionInputStream CreateInputStream(InputStream @in, Decompressor
			 decompressor)
		{
			return new DecompressorStream(@in, decompressor, conf.GetInt("io.file.buffer.size"
				, 4 * 1024));
		}

		public virtual Type GetDecompressorType()
		{
			return ZlibFactory.GetZlibDecompressorType(conf);
		}

		public virtual Decompressor CreateDecompressor()
		{
			return ZlibFactory.GetZlibDecompressor(conf);
		}

		/// <summary><inheritDoc/></summary>
		public virtual DirectDecompressor CreateDirectDecompressor()
		{
			return ZlibFactory.GetZlibDirectDecompressor(conf);
		}

		public virtual string GetDefaultExtension()
		{
			return ".deflate";
		}
	}
}
