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
	/// <summary>
	/// A <a href="http://java.sun.com/javase/6/docs/jdk/api/javadoc/doclet/">Doclet</a>
	/// that only includes class-level elements that are annotated with
	/// <see cref="org.apache.hadoop.classification.InterfaceAudience.Public"/>
	/// .
	/// Class-level elements with no annotation are excluded.
	/// In addition, all elements that are annotated with
	/// <see cref="org.apache.hadoop.classification.InterfaceAudience.Private"/>
	/// or
	/// <see cref="org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate"/>
	/// are also excluded.
	/// It delegates to the Standard Doclet, and takes the same options.
	/// </summary>
	public class IncludePublicAnnotationsStandardDoclet
	{
		public static com.sun.javadoc.LanguageVersion languageVersion()
		{
			return com.sun.javadoc.LanguageVersion.JAVA_1_5;
		}

		public static bool start(com.sun.javadoc.RootDoc root)
		{
			System.Console.Out.WriteLine(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.classification.tools.IncludePublicAnnotationsStandardDoclet
				)).getSimpleName());
			org.apache.hadoop.classification.tools.RootDocProcessor.treatUnannotatedClassesAsPrivate
				 = true;
			return com.sun.tools.doclets.standard.Standard.start(org.apache.hadoop.classification.tools.RootDocProcessor
				.process(root));
		}

		public static int optionLength(string option)
		{
			int length = org.apache.hadoop.classification.tools.StabilityOptions.optionLength
				(option);
			if (length != null)
			{
				return length;
			}
			return com.sun.tools.doclets.standard.Standard.optionLength(option);
		}

		public static bool validOptions(string[][] options, com.sun.javadoc.DocErrorReporter
			 reporter)
		{
			org.apache.hadoop.classification.tools.StabilityOptions.validOptions(options, reporter
				);
			string[][] filteredOptions = org.apache.hadoop.classification.tools.StabilityOptions
				.filterOptions(options);
			return com.sun.tools.doclets.standard.Standard.validOptions(filteredOptions, reporter
				);
		}
	}
}
