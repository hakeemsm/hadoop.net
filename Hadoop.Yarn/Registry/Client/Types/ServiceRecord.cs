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
using System.Text;
using Com.Google.Common.Base;
using Org.Codehaus.Jackson.Annotate;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Types
{
	/// <summary>JSON-marshallable description of a single component.</summary>
	/// <remarks>
	/// JSON-marshallable description of a single component.
	/// It supports the deserialization of unknown attributes, but does
	/// not support their creation.
	/// </remarks>
	public class ServiceRecord : ICloneable
	{
		/// <summary>A type string which MUST be in the serialized json.</summary>
		/// <remarks>
		/// A type string which MUST be in the serialized json. This permits
		/// fast discarding of invalid entries
		/// </remarks>
		public const string RecordType = "JSONServiceRecord";

		/// <summary>The type field.</summary>
		/// <remarks>
		/// The type field. This must be the string
		/// <see cref="RecordType"/>
		/// </remarks>
		public string type = RecordType;

		/// <summary>Description string</summary>
		public string description;

		/// <summary>map to handle unknown attributes.</summary>
		private IDictionary<string, string> attributes = new Dictionary<string, string>(4
			);

		/// <summary>List of endpoints intended for use to external callers</summary>
		public IList<Endpoint> external = new AList<Endpoint>();

		/// <summary>List of endpoints for use <i>within</i> an application.</summary>
		public IList<Endpoint> @internal = new AList<Endpoint>();

		/// <summary>Create a service record with no ID, description or registration time.</summary>
		/// <remarks>
		/// Create a service record with no ID, description or registration time.
		/// Endpoint lists are set to empty lists.
		/// </remarks>
		public ServiceRecord()
		{
		}

		/// <summary>Deep cloning constructor</summary>
		/// <param name="that">service record source</param>
		public ServiceRecord(Org.Apache.Hadoop.Registry.Client.Types.ServiceRecord that)
		{
			this.description = that.description;
			// others
			IDictionary<string, string> thatAttrs = that.attributes;
			foreach (KeyValuePair<string, string> entry in thatAttrs)
			{
				attributes[entry.Key] = entry.Value;
			}
			// endpoints
			IList<Endpoint> src = that.@internal;
			if (src != null)
			{
				@internal = new AList<Endpoint>(src.Count);
				foreach (Endpoint endpoint in src)
				{
					@internal.AddItem(new Endpoint(endpoint));
				}
			}
			src = that.external;
			if (src != null)
			{
				external = new AList<Endpoint>(src.Count);
				foreach (Endpoint endpoint in src)
				{
					external.AddItem(new Endpoint(endpoint));
				}
			}
		}

		/// <summary>Add an external endpoint</summary>
		/// <param name="endpoint">endpoint to set</param>
		public virtual void AddExternalEndpoint(Endpoint endpoint)
		{
			Preconditions.CheckArgument(endpoint != null);
			endpoint.Validate();
			external.AddItem(endpoint);
		}

		/// <summary>Add an internal endpoint</summary>
		/// <param name="endpoint">endpoint to set</param>
		public virtual void AddInternalEndpoint(Endpoint endpoint)
		{
			Preconditions.CheckArgument(endpoint != null);
			endpoint.Validate();
			@internal.AddItem(endpoint);
		}

		/// <summary>Look up an internal endpoint</summary>
		/// <param name="api">API</param>
		/// <returns>the endpoint or null if there was no match</returns>
		public virtual Endpoint GetInternalEndpoint(string api)
		{
			return FindByAPI(@internal, api);
		}

		/// <summary>Look up an external endpoint</summary>
		/// <param name="api">API</param>
		/// <returns>the endpoint or null if there was no match</returns>
		public virtual Endpoint GetExternalEndpoint(string api)
		{
			return FindByAPI(external, api);
		}

		/// <summary>
		/// Handle unknown attributes by storing them in the
		/// <see cref="attributes"/>
		/// map
		/// </summary>
		/// <param name="key">attribute name</param>
		/// <param name="value">attribute value.</param>
		[JsonAnySetter]
		public virtual void Set(string key, object value)
		{
			attributes[key] = value.ToString();
		}

		/// <summary>The map of "other" attributes set when parsing.</summary>
		/// <remarks>
		/// The map of "other" attributes set when parsing. These
		/// are not included in the JSON value of this record when it
		/// is generated.
		/// </remarks>
		/// <returns>a map of any unknown attributes in the deserialized JSON.</returns>
		[JsonAnyGetter]
		public virtual IDictionary<string, string> Attributes()
		{
			return attributes;
		}

		/// <summary>Get the "other" attribute with a specific key</summary>
		/// <param name="key">key to look up</param>
		/// <returns>the value or null</returns>
		public virtual string Get(string key)
		{
			return attributes[key];
		}

		/// <summary>Get the "other" attribute with a specific key.</summary>
		/// <param name="key">key to look up</param>
		/// <param name="defVal">default value</param>
		/// <returns>
		/// the value as a string,
		/// or <code>defval</code> if the value was not present
		/// </returns>
		public virtual string Get(string key, string defVal)
		{
			string val = attributes[key];
			return val != null ? val : defVal;
		}

		/// <summary>Find an endpoint by its API</summary>
		/// <param name="list">list</param>
		/// <param name="api">api name</param>
		/// <returns>the endpoint or null if there was no match</returns>
		private Endpoint FindByAPI(IList<Endpoint> list, string api)
		{
			foreach (Endpoint endpoint in list)
			{
				if (endpoint.api.Equals(api))
				{
					return endpoint;
				}
			}
			return null;
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder("ServiceRecord{");
			sb.Append("description='").Append(description).Append('\'');
			sb.Append("; external endpoints: {");
			foreach (Endpoint endpoint in external)
			{
				sb.Append(endpoint).Append("; ");
			}
			sb.Append("}; internal endpoints: {");
			foreach (Endpoint endpoint_1 in @internal)
			{
				sb.Append(endpoint_1 != null ? endpoint_1.ToString() : "NULL ENDPOINT");
				sb.Append("; ");
			}
			sb.Append('}');
			if (!attributes.IsEmpty())
			{
				sb.Append(", attributes: {");
				foreach (KeyValuePair<string, string> attr in attributes)
				{
					sb.Append("\"").Append(attr.Key).Append("\"=\"").Append(attr.Value).Append("\" ");
				}
			}
			else
			{
				sb.Append(", attributes: {");
			}
			sb.Append('}');
			sb.Append('}');
			return sb.ToString();
		}

		/// <summary>Shallow clone: all endpoints will be shared across instances</summary>
		/// <returns>a clone of the instance</returns>
		/// <exception cref="Sharpen.CloneNotSupportedException"/>
		protected internal virtual object Clone()
		{
			return base.MemberwiseClone();
		}
	}
}
