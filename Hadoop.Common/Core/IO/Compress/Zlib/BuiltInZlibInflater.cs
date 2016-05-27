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
using ICSharpCode.SharpZipLib;
using ICSharpCode.SharpZipLib.Zip.Compression;
using Org.Apache.Hadoop.IO.Compress;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress.Zlib
{
	/// <summary>
	/// A wrapper around java.util.zip.Inflater to make it conform
	/// to org.apache.hadoop.io.compress.Decompressor interface.
	/// </summary>
	public class BuiltInZlibInflater : Inflater, Decompressor
	{
		public BuiltInZlibInflater(bool nowrap)
			: base(nowrap)
		{
		}

		public BuiltInZlibInflater()
			: base()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Decompress(byte[] b, int off, int len)
		{
			lock (this)
			{
				try
				{
					return base.Inflate(b, off, len);
				}
				catch (SharpZipBaseException dfe)
				{
					throw new IOException(dfe.Message);
				}
			}
		}
	}
}
