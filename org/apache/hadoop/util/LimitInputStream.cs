/*
*
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

namespace org.apache.hadoop.util
{
	/// <summary>
	/// Copied from guava source code v15 (LimitedInputStream)
	/// Guava deprecated LimitInputStream in v14 and removed it in v15.
	/// </summary>
	/// <remarks>
	/// Copied from guava source code v15 (LimitedInputStream)
	/// Guava deprecated LimitInputStream in v14 and removed it in v15. Copying this class here
	/// allows to be compatible with guava 11 to 15+.
	/// Originally: org.apache.hadoop.hbase.io.LimitInputStream
	/// </remarks>
	public sealed class LimitInputStream : java.io.FilterInputStream
	{
		private long left;

		private long mark = -1;

		public LimitInputStream(java.io.InputStream @in, long limit)
			: base(@in)
		{
			com.google.common.@base.Preconditions.checkNotNull(@in);
			com.google.common.@base.Preconditions.checkArgument(limit >= 0, "limit must be non-negative"
				);
			left = limit;
		}

		/// <exception cref="System.IO.IOException"/>
		public override int available()
		{
			return (int)System.Math.min(@in.available(), left);
		}

		// it's okay to mark even if mark isn't supported, as reset won't work
		public override void mark(int readLimit)
		{
			lock (this)
			{
				@in.mark(readLimit);
				mark = left;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override int read()
		{
			if (left == 0)
			{
				return -1;
			}
			int result = @in.read();
			if (result != -1)
			{
				--left;
			}
			return result;
		}

		/// <exception cref="System.IO.IOException"/>
		public override int read(byte[] b, int off, int len)
		{
			if (left == 0)
			{
				return -1;
			}
			len = (int)System.Math.min(len, left);
			int result = @in.read(b, off, len);
			if (result != -1)
			{
				left -= result;
			}
			return result;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void reset()
		{
			lock (this)
			{
				if (!@in.markSupported())
				{
					throw new System.IO.IOException("Mark not supported");
				}
				if (mark == -1)
				{
					throw new System.IO.IOException("Mark not set");
				}
				@in.reset();
				left = mark;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override long skip(long n)
		{
			n = System.Math.min(n, left);
			long skipped = @in.skip(n);
			left -= skipped;
			return skipped;
		}
	}
}
