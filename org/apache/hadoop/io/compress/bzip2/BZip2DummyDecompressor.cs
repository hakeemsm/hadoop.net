/*
*  Licensed to the Apache Software Foundation (ASF) under one or more
*  contributor license agreements.  See the NOTICE file distributed with
*  this work for additional information regarding copyright ownership.
*  The ASF licenses this file to You under the Apache License, Version 2.0
*  (the "License"); you may not use this file except in compliance with
*  the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*
*/
using Sharpen;

namespace org.apache.hadoop.io.compress.bzip2
{
	/// <summary>This is a dummy decompressor for BZip2.</summary>
	public class BZip2DummyDecompressor : org.apache.hadoop.io.compress.Decompressor
	{
		/// <exception cref="System.IO.IOException"/>
		public virtual int decompress(byte[] b, int off, int len)
		{
			throw new System.NotSupportedException();
		}

		public virtual void end()
		{
			throw new System.NotSupportedException();
		}

		public virtual bool finished()
		{
			throw new System.NotSupportedException();
		}

		public virtual bool needsDictionary()
		{
			throw new System.NotSupportedException();
		}

		public virtual bool needsInput()
		{
			throw new System.NotSupportedException();
		}

		public virtual int getRemaining()
		{
			throw new System.NotSupportedException();
		}

		public virtual void reset()
		{
		}

		// do nothing
		public virtual void setDictionary(byte[] b, int off, int len)
		{
			throw new System.NotSupportedException();
		}

		public virtual void setInput(byte[] b, int off, int len)
		{
			throw new System.NotSupportedException();
		}
	}
}
