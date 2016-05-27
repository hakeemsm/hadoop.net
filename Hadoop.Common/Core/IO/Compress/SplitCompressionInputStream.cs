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
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress
{
	/// <summary>An InputStream covering a range of compressed data.</summary>
	/// <remarks>
	/// An InputStream covering a range of compressed data. The start and end
	/// offsets requested by a client may be modified by the codec to fit block
	/// boundaries or other algorithm-dependent requirements.
	/// </remarks>
	public abstract class SplitCompressionInputStream : CompressionInputStream
	{
		private long start;

		private long end;

		/// <exception cref="System.IO.IOException"/>
		public SplitCompressionInputStream(InputStream @in, long start, long end)
			: base(@in)
		{
			this.start = start;
			this.end = end;
		}

		protected internal virtual void SetStart(long start)
		{
			this.start = start;
		}

		protected internal virtual void SetEnd(long end)
		{
			this.end = end;
		}

		/// <summary>
		/// After calling createInputStream, the values of start or end
		/// might change.
		/// </summary>
		/// <remarks>
		/// After calling createInputStream, the values of start or end
		/// might change.  So this method can be used to get the new value of start.
		/// </remarks>
		/// <returns>The changed value of start</returns>
		public virtual long GetAdjustedStart()
		{
			return start;
		}

		/// <summary>
		/// After calling createInputStream, the values of start or end
		/// might change.
		/// </summary>
		/// <remarks>
		/// After calling createInputStream, the values of start or end
		/// might change.  So this method can be used to get the new value of end.
		/// </remarks>
		/// <returns>The changed value of end</returns>
		public virtual long GetAdjustedEnd()
		{
			return end;
		}
	}
}
