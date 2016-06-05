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
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.FI
{
	/// <summary>
	/// This class is responsible for the decision of when a fault
	/// has to be triggered within a class of Hadoop
	/// Default probability of injection is set to 0%.
	/// </summary>
	/// <remarks>
	/// This class is responsible for the decision of when a fault
	/// has to be triggered within a class of Hadoop
	/// Default probability of injection is set to 0%. To change it
	/// one can set the sys. prop. -Dfi.*=<new probability level>
	/// Another way to do so is to set this level through FI config file,
	/// located under src/test/fi-site.conf
	/// To change the level one has to specify the following sys,prop.:
	/// -Dfi.<name of fault location>=<probability level> in the runtime
	/// Probability level is specified by a float between 0.0 and 1.0
	/// <name of fault location> might be represented by a short classname
	/// or otherwise. This decision is left up to the discretion of aspects
	/// developer, but has to be consistent through the code
	/// </remarks>
	public class ProbabilityModel
	{
		private static Random generator = new Random();

		private static readonly Log Log = LogFactory.GetLog(typeof(ProbabilityModel));

		internal const string FprobName = "fi.";

		private const string AllProbabilities = FprobName + "*";

		private const float DefaultProb = 0.00f;

		private const float MaxProb = 1.00f;

		private static Configuration conf = FiConfig.GetConfig();

		static ProbabilityModel()
		{
			//Default probability is 0%
			// Max probability is 100%
			// Set new default probability if specified through a system.property
			// If neither is specified set default probability to DEFAULT_PROB 
			conf.Set(AllProbabilities, Runtime.GetProperty(AllProbabilities, conf.Get(AllProbabilities
				, float.ToString(DefaultProb))));
			Log.Info(AllProbabilities + "=" + conf.Get(AllProbabilities));
		}

		/// <summary>Simplistic method to check if we have reached the point of injection</summary>
		/// <param name="klassName">
		/// is the name of the probability level to check.
		/// If a configuration has been set for "fi.myClass" then you can check if the
		/// inject criteria has been reached by calling this method with "myClass"
		/// string as its parameter
		/// </param>
		/// <returns>true if the probability threshold has been reached; false otherwise</returns>
		public static bool InjectCriteria(string klassName)
		{
			bool trigger = false;
			// TODO fix this: make it more sophisticated!!!
			if (generator.NextFloat() < GetProbability(klassName))
			{
				trigger = true;
			}
			return trigger;
		}

		/// <summary>This primitive checks for arbitrary set of desired probability.</summary>
		/// <remarks>
		/// This primitive checks for arbitrary set of desired probability. If the
		/// level hasn't been set method will return default setting.
		/// The probability expected to be set as an float between 0.0 and 1.0
		/// </remarks>
		/// <param name="klass">is the name of the resource</param>
		/// <returns>
		/// float representation of configured probability level of
		/// the requested resource or default value if hasn't been set
		/// </returns>
		protected internal static float GetProbability(string klass)
		{
			string newProbName = FprobName + klass;
			string newValue = Runtime.GetProperty(newProbName, conf.Get(AllProbabilities));
			if (newValue != null && !newValue.Equals(conf.Get(newProbName)))
			{
				conf.Set(newProbName, newValue);
			}
			float ret = conf.GetFloat(newProbName, conf.GetFloat(AllProbabilities, DefaultProb
				));
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Request for " + newProbName + " returns=" + ret);
			}
			// Make sure that probability level is valid.
			if (ret < DefaultProb || ret > MaxProb)
			{
				ret = conf.GetFloat(AllProbabilities, DefaultProb);
			}
			return ret;
		}
	}
}
