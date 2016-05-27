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
using System.Collections.Generic;
using Com.Sun.Javadoc;
using Sharpen;

namespace Org.Apache.Hadoop.Classification.Tools
{
	internal class StabilityOptions
	{
		public const string StableOption = "-stable";

		public const string EvolvingOption = "-evolving";

		public const string UnstableOption = "-unstable";

		public static int OptionLength(string option)
		{
			string opt = option.ToLower(Sharpen.Extensions.GetEnglishCulture());
			if (opt.Equals(UnstableOption))
			{
				return 1;
			}
			if (opt.Equals(EvolvingOption))
			{
				return 1;
			}
			if (opt.Equals(StableOption))
			{
				return 1;
			}
			return null;
		}

		public static void ValidOptions(string[][] options, DocErrorReporter reporter)
		{
			for (int i = 0; i < options.Length; i++)
			{
				string opt = options[i][0].ToLower(Sharpen.Extensions.GetEnglishCulture());
				if (opt.Equals(UnstableOption))
				{
					RootDocProcessor.stability = UnstableOption;
				}
				else
				{
					if (opt.Equals(EvolvingOption))
					{
						RootDocProcessor.stability = EvolvingOption;
					}
					else
					{
						if (opt.Equals(StableOption))
						{
							RootDocProcessor.stability = StableOption;
						}
					}
				}
			}
		}

		public static string[][] FilterOptions(string[][] options)
		{
			IList<string[]> optionsList = new AList<string[]>();
			for (int i = 0; i < options.Length; i++)
			{
				if (!Sharpen.Runtime.EqualsIgnoreCase(options[i][0], UnstableOption) && !Sharpen.Runtime.EqualsIgnoreCase
					(options[i][0], EvolvingOption) && !Sharpen.Runtime.EqualsIgnoreCase(options[i][
					0], StableOption))
				{
					optionsList.AddItem(options[i]);
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
