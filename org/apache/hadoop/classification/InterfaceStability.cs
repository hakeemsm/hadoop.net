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

namespace org.apache.hadoop.classification
{
	/// <summary>
	/// Annotation to inform users of how much to rely on a particular package,
	/// class or method not changing over time.
	/// </summary>
	/// <remarks>
	/// Annotation to inform users of how much to rely on a particular package,
	/// class or method not changing over time. Currently the stability can be
	/// <see cref="Stable"/>
	/// ,
	/// <see cref="Evolving"/>
	/// or
	/// <see cref="Unstable"/>
	/// . <br />
	/// <ul><li>All classes that are annotated with
	/// <see cref="Public"/>
	/// or
	/// <see cref="LimitedPrivate"/>
	/// must have InterfaceStability annotation. </li>
	/// <li>Classes that are
	/// <see cref="Private"/>
	/// are to be considered unstable unless
	/// a different InterfaceStability annotation states otherwise.</li>
	/// <li>Incompatible changes must not be made to classes marked as stable.</li>
	/// </ul>
	/// </remarks>
	public class InterfaceStability
	{
	}
}
