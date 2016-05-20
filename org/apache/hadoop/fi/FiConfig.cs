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

namespace org.apache.hadoop.fi
{
	/// <summary>
	/// This class wraps the logic around fault injection configuration file
	/// Default file is expected to be found in src/test/fi-site.xml
	/// This default file should be copied by JUnit Ant's tasks to
	/// build/test/extraconf folder before tests are ran
	/// An alternative location can be set through
	/// -Dfi.config=<file_name>
	/// </summary>
	public class FiConfig
	{
		private const string CONFIG_PARAMETER = org.apache.hadoop.fi.ProbabilityModel.FPROB_NAME
			 + "config";

		private const string DEFAULT_CONFIG = "fi-site.xml";

		private static org.apache.hadoop.conf.Configuration conf;

		static FiConfig()
		{
			if (conf == null)
			{
				conf = new org.apache.hadoop.conf.Configuration(false);
				string configName = Sharpen.Runtime.getProperty(CONFIG_PARAMETER, DEFAULT_CONFIG);
				conf.addResource(configName);
			}
		}

		/// <summary>Method provides access to local Configuration</summary>
		/// <returns>Configuration initialized with fault injection's parameters</returns>
		public static org.apache.hadoop.conf.Configuration getConfig()
		{
			return conf;
		}
	}
}
