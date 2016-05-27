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
using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>Experimental interface to extend metrics dynamically</summary>
	[System.Serializable]
	public sealed class DefaultMetricsFactory
	{
		public static readonly DefaultMetricsFactory Instance = new DefaultMetricsFactory
			();

		private MutableMetricsFactory mmfImpl;

		// the singleton
		public static MutableMetricsFactory GetAnnotatedMetricsFactory()
		{
			return DefaultMetricsFactory.Instance.GetInstance<MutableMetricsFactory>();
		}

		public T GetInstance<T>()
		{
			System.Type cls = typeof(T);
			lock (this)
			{
				if (cls == typeof(MutableMetricsFactory))
				{
					if (DefaultMetricsFactory.mmfImpl == null)
					{
						DefaultMetricsFactory.mmfImpl = new MutableMetricsFactory();
					}
					return (T)DefaultMetricsFactory.mmfImpl;
				}
				throw new MetricsException("Unknown metrics factory type: " + cls.FullName);
			}
		}

		public void SetInstance(MutableMetricsFactory factory)
		{
			lock (this)
			{
				DefaultMetricsFactory.mmfImpl = factory;
			}
		}
	}
}
