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
using System;
using System.Collections.Generic;
using System.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.State
{
	public class VisualizeStateMachine
	{
		/// <param name="classes">
		/// list of classes which have static field
		/// stateMachineFactory of type StateMachineFactory
		/// </param>
		/// <returns>graph represent this StateMachine</returns>
		/// <exception cref="System.Exception"/>
		public static Graph GetGraphFromClasses(string graphName, IList<string> classes)
		{
			Graph ret = null;
			if (classes.Count != 1)
			{
				ret = new Graph(graphName);
			}
			foreach (string className in classes)
			{
				Type clz = Sharpen.Runtime.GetType(className);
				FieldInfo factoryField = Sharpen.Runtime.GetDeclaredField(clz, "stateMachineFactory"
					);
				StateMachineFactory factory = (StateMachineFactory)factoryField.GetValue(null);
				if (classes.Count == 1)
				{
					return factory.GenerateStateGraph(graphName);
				}
				string gname = clz.Name;
				if (gname.EndsWith("Impl"))
				{
					gname = Sharpen.Runtime.Substring(gname, 0, gname.Length - 4);
				}
				ret.AddSubGraph(factory.GenerateStateGraph(gname));
			}
			return ret;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			if (args.Length < 3)
			{
				System.Console.Error.Printf("Usage: %s <GraphName> <class[,class[,...]]> <OutputFile>%n"
					, typeof(VisualizeStateMachine).FullName);
				System.Environment.Exit(1);
			}
			string[] classes = args[1].Split(",");
			AList<string> validClasses = new AList<string>();
			foreach (string c in classes)
			{
				string vc = c.Trim();
				if (vc.Length > 0)
				{
					validClasses.AddItem(vc);
				}
			}
			Graph g = GetGraphFromClasses(args[0], validClasses);
			g.Save(args[2]);
		}
	}
}
