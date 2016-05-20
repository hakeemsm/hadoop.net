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

namespace org.apache.hadoop.metrics2.lib
{
	/// <summary>Experimental interface to extend metrics dynamically</summary>
	[System.Serializable]
	public sealed class DefaultMetricsFactory
	{
		public static readonly org.apache.hadoop.metrics2.lib.DefaultMetricsFactory INSTANCE
			 = new org.apache.hadoop.metrics2.lib.DefaultMetricsFactory();

		private org.apache.hadoop.metrics2.lib.MutableMetricsFactory mmfImpl;

		// the singleton
		public static org.apache.hadoop.metrics2.lib.MutableMetricsFactory getAnnotatedMetricsFactory
			()
		{
			return org.apache.hadoop.metrics2.lib.DefaultMetricsFactory.INSTANCE.getInstance<
				org.apache.hadoop.metrics2.lib.MutableMetricsFactory>();
		}

		public T getInstance<T>()
		{
			System.Type cls = typeof(T);
			lock (this)
			{
				if (cls == Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.lib.MutableMetricsFactory
					)))
				{
					if (org.apache.hadoop.metrics2.lib.DefaultMetricsFactory.mmfImpl == null)
					{
						org.apache.hadoop.metrics2.lib.DefaultMetricsFactory.mmfImpl = new org.apache.hadoop.metrics2.lib.MutableMetricsFactory
							();
					}
					return (T)org.apache.hadoop.metrics2.lib.DefaultMetricsFactory.mmfImpl;
				}
				throw new org.apache.hadoop.metrics2.MetricsException("Unknown metrics factory type: "
					 + cls.getName());
			}
		}

		public void setInstance(org.apache.hadoop.metrics2.lib.MutableMetricsFactory factory
			)
		{
			lock (this)
			{
				org.apache.hadoop.metrics2.lib.DefaultMetricsFactory.mmfImpl = factory;
			}
		}
	}
}
