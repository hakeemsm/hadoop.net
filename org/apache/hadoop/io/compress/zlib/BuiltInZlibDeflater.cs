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

namespace org.apache.hadoop.io.compress.zlib
{
	/// <summary>
	/// A wrapper around java.util.zip.Deflater to make it conform
	/// to org.apache.hadoop.io.compress.Compressor interface.
	/// </summary>
	public class BuiltInZlibDeflater : java.util.zip.Deflater, org.apache.hadoop.io.compress.Compressor
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.compress.zlib.BuiltInZlibDeflater
			)));

		public BuiltInZlibDeflater(int level, bool nowrap)
			: base(level, nowrap)
		{
		}

		public BuiltInZlibDeflater(int level)
			: base(level)
		{
		}

		public BuiltInZlibDeflater()
			: base()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int compress(byte[] b, int off, int len)
		{
			lock (this)
			{
				return base.deflate(b, off, len);
			}
		}

		/// <summary>reinit the compressor with the given configuration.</summary>
		/// <remarks>
		/// reinit the compressor with the given configuration. It will reset the
		/// compressor's compression level and compression strategy. Different from
		/// <tt>ZlibCompressor</tt>, <tt>BuiltInZlibDeflater</tt> only support three
		/// kind of compression strategy: FILTERED, HUFFMAN_ONLY and DEFAULT_STRATEGY.
		/// It will use DEFAULT_STRATEGY as default if the configured compression
		/// strategy is not supported.
		/// </remarks>
		public virtual void reinit(org.apache.hadoop.conf.Configuration conf)
		{
			reset();
			if (conf == null)
			{
				return;
			}
			setLevel(org.apache.hadoop.io.compress.zlib.ZlibFactory.getCompressionLevel(conf)
				.compressionLevel());
			org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionStrategy strategy = 
				org.apache.hadoop.io.compress.zlib.ZlibFactory.getCompressionStrategy(conf);
			try
			{
				setStrategy(strategy.compressionStrategy());
			}
			catch (System.ArgumentException)
			{
				LOG.warn(strategy + " not supported by BuiltInZlibDeflater.");
				setStrategy(DEFAULT_STRATEGY);
			}
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Reinit compressor with new compression configuration");
			}
		}
	}
}
