/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System;
using System.Collections.Generic;
using System.IO;
using Javax.Management;
using Javax.Management.Openmbean;
using Javax.Servlet.Http;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Http;
using Org.Codehaus.Jackson;
using Sharpen;
using Sharpen.Management;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Jmx
{
	/// <summary>Provides Read only web access to JMX.</summary>
	/// <remarks>
	/// Provides Read only web access to JMX.
	/// <p>
	/// This servlet generally will be placed under the /jmx URL for each
	/// HttpServer.  It provides read only
	/// access to JMX metrics.  The optional <code>qry</code> parameter
	/// may be used to query only a subset of the JMX Beans.  This query
	/// functionality is provided through the
	/// <see cref="Javax.Management.MBeanServer.QueryNames(Javax.Management.ObjectName, Javax.Management.QueryExp)
	/// 	"/>
	/// method.
	/// <p>
	/// For example <code>http://.../jmx?qry=Hadoop:*</code> will return
	/// all hadoop metrics exposed through JMX.
	/// <p>
	/// The optional <code>get</code> parameter is used to query an specific
	/// attribute of a JMX bean.  The format of the URL is
	/// <code>http://.../jmx?get=MXBeanName::AttributeName<code>
	/// <p>
	/// For example
	/// <code>
	/// http://../jmx?get=Hadoop:service=NameNode,name=NameNodeInfo::ClusterId
	/// </code> will return the cluster id of the namenode mxbean.
	/// <p>
	/// If the <code>qry</code> or the <code>get</code> parameter is not formatted
	/// correctly then a 400 BAD REQUEST http response code will be returned.
	/// <p>
	/// If a resouce such as a mbean or attribute can not be found,
	/// a 404 SC_NOT_FOUND http response code will be returned.
	/// <p>
	/// The return format is JSON and in the form
	/// <p>
	/// <code><pre>
	/// {
	/// "beans" : [
	/// {
	/// "name":"bean-name"
	/// ...
	/// }
	/// ]
	/// }
	/// </pre></code>
	/// <p>
	/// The servlet attempts to convert the the JMXBeans into JSON. Each
	/// bean's attributes will be converted to a JSON object member.
	/// If the attribute is a boolean, a number, a string, or an array
	/// it will be converted to the JSON equivalent.
	/// If the value is a
	/// <see cref="Javax.Management.Openmbean.CompositeData"/>
	/// then it will be converted
	/// to a JSON object with the keys as the name of the JSON member and
	/// the value is converted following these same rules.
	/// If the value is a
	/// <see cref="Javax.Management.Openmbean.TabularData"/>
	/// then it will be converted
	/// to an array of the
	/// <see cref="Javax.Management.Openmbean.CompositeData"/>
	/// elements that it contains.
	/// All other objects will be converted to a string and output as such.
	/// The bean's name and modelerType will be returned for all beans.
	/// </remarks>
	[System.Serializable]
	public class JMXJsonServlet : HttpServlet
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(JMXJsonServlet));

		internal const string AccessControlAllowMethods = "Access-Control-Allow-Methods";

		internal const string AccessControlAllowOrigin = "Access-Control-Allow-Origin";

		private const long serialVersionUID = 1L;

		/// <summary>MBean server.</summary>
		[System.NonSerialized]
		protected internal MBeanServer mBeanServer = null;

		/*
		* This servlet is based off of the JMXProxyServlet from Tomcat 7.0.14. It has
		* been rewritten to be read only and to output in a JSON format so it is not
		* really that close to the original.
		*/
		// --------------------------------------------------------- Public Methods
		/// <summary>Initialize this servlet.</summary>
		/// <exception cref="Javax.Servlet.ServletException"/>
		public override void Init()
		{
			// Retrieve the MBean server
			mBeanServer = ManagementFactory.GetPlatformMBeanServer();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual bool IsInstrumentationAccessAllowed(HttpServletRequest
			 request, HttpServletResponse response)
		{
			return HttpServer2.IsInstrumentationAccessAllowed(GetServletContext(), request, response
				);
		}

		/// <summary>Process a GET request for the specified resource.</summary>
		/// <param name="request">The servlet request we are processing</param>
		/// <param name="response">The servlet response we are creating</param>
		protected override void DoGet(HttpServletRequest request, HttpServletResponse response
			)
		{
			string jsonpcb = null;
			PrintWriter writer = null;
			try
			{
				if (!IsInstrumentationAccessAllowed(request, response))
				{
					return;
				}
				JsonGenerator jg = null;
				try
				{
					writer = response.GetWriter();
					response.SetContentType("application/json; charset=utf8");
					response.SetHeader(AccessControlAllowMethods, "GET");
					response.SetHeader(AccessControlAllowOrigin, "*");
					JsonFactory jsonFactory = new JsonFactory();
					jg = jsonFactory.CreateJsonGenerator(writer);
					jg.Disable(JsonGenerator.Feature.AutoCloseTarget);
					jg.UseDefaultPrettyPrinter();
					jg.WriteStartObject();
					if (mBeanServer == null)
					{
						jg.WriteStringField("result", "ERROR");
						jg.WriteStringField("message", "No MBeanServer could be found");
						jg.Close();
						Log.Error("No MBeanServer could be found.");
						response.SetStatus(HttpServletResponse.ScNotFound);
						return;
					}
					// query per mbean attribute
					string getmethod = request.GetParameter("get");
					if (getmethod != null)
					{
						string[] splitStrings = getmethod.Split("\\:\\:");
						if (splitStrings.Length != 2)
						{
							jg.WriteStringField("result", "ERROR");
							jg.WriteStringField("message", "query format is not as expected.");
							jg.Close();
							response.SetStatus(HttpServletResponse.ScBadRequest);
							return;
						}
						ListBeans(jg, new ObjectName(splitStrings[0]), splitStrings[1], response);
						jg.Close();
						return;
					}
					// query per mbean
					string qry = request.GetParameter("qry");
					if (qry == null)
					{
						qry = "*:*";
					}
					ListBeans(jg, new ObjectName(qry), null, response);
				}
				finally
				{
					if (jg != null)
					{
						jg.Close();
					}
					if (writer != null)
					{
						writer.Close();
					}
				}
			}
			catch (IOException e)
			{
				Log.Error("Caught an exception while processing JMX request", e);
				response.SetStatus(HttpServletResponse.ScInternalServerError);
			}
			catch (MalformedObjectNameException e)
			{
				Log.Error("Caught an exception while processing JMX request", e);
				response.SetStatus(HttpServletResponse.ScBadRequest);
			}
			finally
			{
				if (writer != null)
				{
					writer.Close();
				}
			}
		}

		// --------------------------------------------------------- Private Methods
		/// <exception cref="System.IO.IOException"/>
		private void ListBeans(JsonGenerator jg, ObjectName qry, string attribute, HttpServletResponse
			 response)
		{
			Log.Debug("Listing beans for " + qry);
			ICollection<ObjectName> names = null;
			names = mBeanServer.QueryNames(qry, null);
			jg.WriteArrayFieldStart("beans");
			IEnumerator<ObjectName> it = names.GetEnumerator();
			while (it.HasNext())
			{
				ObjectName oname = it.Next();
				MBeanInfo minfo;
				string code = string.Empty;
				object attributeinfo = null;
				try
				{
					minfo = mBeanServer.GetMBeanInfo(oname);
					code = minfo.GetClassName();
					string prs = string.Empty;
					try
					{
						if ("org.apache.commons.modeler.BaseModelMBean".Equals(code))
						{
							prs = "modelerType";
							code = (string)mBeanServer.GetAttribute(oname, prs);
						}
						if (attribute != null)
						{
							prs = attribute;
							attributeinfo = mBeanServer.GetAttribute(oname, prs);
						}
					}
					catch (AttributeNotFoundException e)
					{
						// If the modelerType attribute was not found, the class name is used
						// instead.
						Log.Error("getting attribute " + prs + " of " + oname + " threw an exception", e);
					}
					catch (MBeanException e)
					{
						// The code inside the attribute getter threw an exception so log it,
						// and fall back on the class name
						Log.Error("getting attribute " + prs + " of " + oname + " threw an exception", e);
					}
					catch (RuntimeException e)
					{
						// For some reason even with an MBeanException available to them
						// Runtime exceptionscan still find their way through, so treat them
						// the same as MBeanException
						Log.Error("getting attribute " + prs + " of " + oname + " threw an exception", e);
					}
					catch (ReflectionException e)
					{
						// This happens when the code inside the JMX bean (setter?? from the
						// java docs) threw an exception, so log it and fall back on the 
						// class name
						Log.Error("getting attribute " + prs + " of " + oname + " threw an exception", e);
					}
				}
				catch (InstanceNotFoundException)
				{
					//Ignored for some reason the bean was not found so don't output it
					continue;
				}
				catch (IntrospectionException e)
				{
					// This is an internal error, something odd happened with reflection so
					// log it and don't output the bean.
					Log.Error("Problem while trying to process JMX query: " + qry + " with MBean " + 
						oname, e);
					continue;
				}
				catch (ReflectionException e)
				{
					// This happens when the code inside the JMX bean threw an exception, so
					// log it and don't output the bean.
					Log.Error("Problem while trying to process JMX query: " + qry + " with MBean " + 
						oname, e);
					continue;
				}
				jg.WriteStartObject();
				jg.WriteStringField("name", oname.ToString());
				jg.WriteStringField("modelerType", code);
				if ((attribute != null) && (attributeinfo == null))
				{
					jg.WriteStringField("result", "ERROR");
					jg.WriteStringField("message", "No attribute with name " + attribute + " was found."
						);
					jg.WriteEndObject();
					jg.WriteEndArray();
					jg.Close();
					response.SetStatus(HttpServletResponse.ScNotFound);
					return;
				}
				if (attribute != null)
				{
					WriteAttribute(jg, attribute, attributeinfo);
				}
				else
				{
					MBeanAttributeInfo[] attrs = minfo.GetAttributes();
					for (int i = 0; i < attrs.Length; i++)
					{
						WriteAttribute(jg, oname, attrs[i]);
					}
				}
				jg.WriteEndObject();
			}
			jg.WriteEndArray();
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteAttribute(JsonGenerator jg, ObjectName oname, MBeanAttributeInfo
			 attr)
		{
			if (!attr.IsReadable())
			{
				return;
			}
			string attName = attr.GetName();
			if ("modelerType".Equals(attName))
			{
				return;
			}
			if (attName.IndexOf("=") >= 0 || attName.IndexOf(":") >= 0 || attName.IndexOf(" "
				) >= 0)
			{
				return;
			}
			object value = null;
			try
			{
				value = mBeanServer.GetAttribute(oname, attName);
			}
			catch (RuntimeMBeanException e)
			{
				// UnsupportedOperationExceptions happen in the normal course of business,
				// so no need to log them as errors all the time.
				if (e.InnerException is NotSupportedException)
				{
					Log.Debug("getting attribute " + attName + " of " + oname + " threw an exception"
						, e);
				}
				else
				{
					Log.Error("getting attribute " + attName + " of " + oname + " threw an exception"
						, e);
				}
				return;
			}
			catch (RuntimeErrorException e)
			{
				// RuntimeErrorException happens when an unexpected failure occurs in getAttribute
				// for example https://issues.apache.org/jira/browse/DAEMON-120
				Log.Debug("getting attribute " + attName + " of " + oname + " threw an exception"
					, e);
				return;
			}
			catch (AttributeNotFoundException)
			{
				//Ignored the attribute was not found, which should never happen because the bean
				//just told us that it has this attribute, but if this happens just don't output
				//the attribute.
				return;
			}
			catch (MBeanException e)
			{
				//The code inside the attribute getter threw an exception so log it, and
				// skip outputting the attribute
				Log.Error("getting attribute " + attName + " of " + oname + " threw an exception"
					, e);
				return;
			}
			catch (RuntimeException e)
			{
				//For some reason even with an MBeanException available to them Runtime exceptions
				//can still find their way through, so treat them the same as MBeanException
				Log.Error("getting attribute " + attName + " of " + oname + " threw an exception"
					, e);
				return;
			}
			catch (ReflectionException e)
			{
				//This happens when the code inside the JMX bean (setter?? from the java docs)
				//threw an exception, so log it and skip outputting the attribute
				Log.Error("getting attribute " + attName + " of " + oname + " threw an exception"
					, e);
				return;
			}
			catch (InstanceNotFoundException)
			{
				//Ignored the mbean itself was not found, which should never happen because we
				//just accessed it (perhaps something unregistered in-between) but if this
				//happens just don't output the attribute.
				return;
			}
			WriteAttribute(jg, attName, value);
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteAttribute(JsonGenerator jg, string attName, object value)
		{
			jg.WriteFieldName(attName);
			WriteObject(jg, value);
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteObject(JsonGenerator jg, object value)
		{
			if (value == null)
			{
				jg.WriteNull();
			}
			else
			{
				Type c = value.GetType();
				if (c.IsArray)
				{
					jg.WriteStartArray();
					int len = Sharpen.Runtime.GetArrayLength(value);
					for (int j = 0; j < len; j++)
					{
						object item = Sharpen.Runtime.GetArrayValue(value, j);
						WriteObject(jg, item);
					}
					jg.WriteEndArray();
				}
				else
				{
					if (value is Number)
					{
						Number n = (Number)value;
						jg.WriteNumber(n.ToString());
					}
					else
					{
						if (value is bool)
						{
							bool b = (bool)value;
							jg.WriteBoolean(b);
						}
						else
						{
							if (value is CompositeData)
							{
								CompositeData cds = (CompositeData)value;
								CompositeType comp = cds.GetCompositeType();
								ICollection<string> keys = comp.KeySet();
								jg.WriteStartObject();
								foreach (string key in keys)
								{
									WriteAttribute(jg, key, cds.Get(key));
								}
								jg.WriteEndObject();
							}
							else
							{
								if (value is TabularData)
								{
									TabularData tds = (TabularData)value;
									jg.WriteStartArray();
									foreach (object entry in tds.Values())
									{
										WriteObject(jg, entry);
									}
									jg.WriteEndArray();
								}
								else
								{
									jg.WriteString(value.ToString());
								}
							}
						}
					}
				}
			}
		}
	}
}
