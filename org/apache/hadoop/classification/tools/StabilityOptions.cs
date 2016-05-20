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

namespace org.apache.hadoop.classification.tools
{
	internal class StabilityOptions
	{
		public const string STABLE_OPTION = "-stable";

		public const string EVOLVING_OPTION = "-evolving";

		public const string UNSTABLE_OPTION = "-unstable";

		public static int optionLength(string option)
		{
			string opt = option.ToLower(java.util.Locale.ENGLISH);
			if (opt.Equals(UNSTABLE_OPTION))
			{
				return 1;
			}
			if (opt.Equals(EVOLVING_OPTION))
			{
				return 1;
			}
			if (opt.Equals(STABLE_OPTION))
			{
				return 1;
			}
			return null;
		}

		public static void validOptions(string[][] options, com.sun.javadoc.DocErrorReporter
			 reporter)
		{
			for (int i = 0; i < options.Length; i++)
			{
				string opt = options[i][0].ToLower(java.util.Locale.ENGLISH);
				if (opt.Equals(UNSTABLE_OPTION))
				{
					org.apache.hadoop.classification.tools.RootDocProcessor.stability = UNSTABLE_OPTION;
				}
				else
				{
					if (opt.Equals(EVOLVING_OPTION))
					{
						org.apache.hadoop.classification.tools.RootDocProcessor.stability = EVOLVING_OPTION;
					}
					else
					{
						if (opt.Equals(STABLE_OPTION))
						{
							org.apache.hadoop.classification.tools.RootDocProcessor.stability = STABLE_OPTION;
						}
					}
				}
			}
		}

		public static string[][] filterOptions(string[][] options)
		{
			System.Collections.Generic.IList<string[]> optionsList = new System.Collections.Generic.List
				<string[]>();
			for (int i = 0; i < options.Length; i++)
			{
				if (!Sharpen.Runtime.equalsIgnoreCase(options[i][0], UNSTABLE_OPTION) && !Sharpen.Runtime.equalsIgnoreCase
					(options[i][0], EVOLVING_OPTION) && !Sharpen.Runtime.equalsIgnoreCase(options[i]
					[0], STABLE_OPTION))
				{
					optionsList.add(options[i]);
				}
			}
			string[][] filteredOptions = new string[optionsList.Count][];
			int i_1 = 0;
			foreach (string[] option in optionsList)
			{
				filteredOptions[i_1++] = option;
			}
			return filteredOptions;
		}
	}
}
