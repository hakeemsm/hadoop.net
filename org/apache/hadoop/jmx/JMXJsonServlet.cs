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
using Sharpen;

namespace org.apache.hadoop.jmx
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
	/// <see cref="javax.management.MBeanServer.queryNames(javax.management.ObjectName, javax.management.QueryExp)
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
	/// <see cref="javax.management.openmbean.CompositeData"/>
	/// then it will be converted
	/// to a JSON object with the keys as the name of the JSON member and
	/// the value is converted following these same rules.
	/// If the value is a
	/// <see cref="javax.management.openmbean.TabularData"/>
	/// then it will be converted
	/// to an array of the
	/// <see cref="javax.management.openmbean.CompositeData"/>
	/// elements that it contains.
	/// All other objects will be converted to a string and output as such.
	/// The bean's name and modelerType will be returned for all beans.
	/// </remarks>
	[System.Serializable]
	public class JMXJsonServlet : javax.servlet.http.HttpServlet
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.jmx.JMXJsonServlet
			)));

		internal const string ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";

		internal const string ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";

		private const long serialVersionUID = 1L;

		/// <summary>MBean server.</summary>
		[System.NonSerialized]
		protected internal javax.management.MBeanServer mBeanServer = null;

		/*
		* This servlet is based off of the JMXProxyServlet from Tomcat 7.0.14. It has
		* been rewritten to be read only and to output in a JSON format so it is not
		* really that close to the original.
		*/
		// --------------------------------------------------------- Public Methods
		/// <summary>Initialize this servlet.</summary>
		/// <exception cref="javax.servlet.ServletException"/>
		public override void init()
		{
			// Retrieve the MBean server
			mBeanServer = java.lang.management.ManagementFactory.getPlatformMBeanServer();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual bool isInstrumentationAccessAllowed(javax.servlet.http.HttpServletRequest
			 request, javax.servlet.http.HttpServletResponse response)
		{
			return org.apache.hadoop.http.HttpServer2.isInstrumentationAccessAllowed(getServletContext
				(), request, response);
		}

		/// <summary>Process a GET request for the specified resource.</summary>
		/// <param name="request">The servlet request we are processing</param>
		/// <param name="response">The servlet response we are creating</param>
		protected override void doGet(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
			 response)
		{
			string jsonpcb = null;
			java.io.PrintWriter writer = null;
			try
			{
				if (!isInstrumentationAccessAllowed(request, response))
				{
					return;
				}
				org.codehaus.jackson.JsonGenerator jg = null;
				try
				{
					writer = response.getWriter();
					response.setContentType("application/json; charset=utf8");
					response.setHeader(ACCESS_CONTROL_ALLOW_METHODS, "GET");
					response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
					org.codehaus.jackson.JsonFactory jsonFactory = new org.codehaus.jackson.JsonFactory
						();
					jg = jsonFactory.createJsonGenerator(writer);
					jg.disable(org.codehaus.jackson.JsonGenerator.Feature.AUTO_CLOSE_TARGET);
					jg.useDefaultPrettyPrinter();
					jg.writeStartObject();
					if (mBeanServer == null)
					{
						jg.writeStringField("result", "ERROR");
						jg.writeStringField("message", "No MBeanServer could be found");
						jg.close();
						LOG.error("No MBeanServer could be found.");
						response.setStatus(javax.servlet.http.HttpServletResponse.SC_NOT_FOUND);
						return;
					}
					// query per mbean attribute
					string getmethod = request.getParameter("get");
					if (getmethod != null)
					{
						string[] splitStrings = getmethod.split("\\:\\:");
						if (splitStrings.Length != 2)
						{
							jg.writeStringField("result", "ERROR");
							jg.writeStringField("message", "query format is not as expected.");
							jg.close();
							response.setStatus(javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST);
							return;
						}
						listBeans(jg, new javax.management.ObjectName(splitStrings[0]), splitStrings[1], 
							response);
						jg.close();
						return;
					}
					// query per mbean
					string qry = request.getParameter("qry");
					if (qry == null)
					{
						qry = "*:*";
					}
					listBeans(jg, new javax.management.ObjectName(qry), null, response);
				}
				finally
				{
					if (jg != null)
					{
						jg.close();
					}
					if (writer != null)
					{
						writer.close();
					}
				}
			}
			catch (System.IO.IOException e)
			{
				LOG.error("Caught an exception while processing JMX request", e);
				response.setStatus(javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR
					);
			}
			catch (javax.management.MalformedObjectNameException e)
			{
				LOG.error("Caught an exception while processing JMX request", e);
				response.setStatus(javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST);
			}
			finally
			{
				if (writer != null)
				{
					writer.close();
				}
			}
		}

		// --------------------------------------------------------- Private Methods
		/// <exception cref="System.IO.IOException"/>
		private void listBeans(org.codehaus.jackson.JsonGenerator jg, javax.management.ObjectName
			 qry, string attribute, javax.servlet.http.HttpServletResponse response)
		{
			LOG.debug("Listing beans for " + qry);
			System.Collections.Generic.ICollection<javax.management.ObjectName> names = null;
			names = mBeanServer.queryNames(qry, null);
			jg.writeArrayFieldStart("beans");
			System.Collections.Generic.IEnumerator<javax.management.ObjectName> it = names.GetEnumerator
				();
			while (it.MoveNext())
			{
				javax.management.ObjectName oname = it.Current;
				javax.management.MBeanInfo minfo;
				string code = string.Empty;
				object attributeinfo = null;
				try
				{
					minfo = mBeanServer.getMBeanInfo(oname);
					code = minfo.getClassName();
					string prs = string.Empty;
					try
					{
						if ("org.apache.commons.modeler.BaseModelMBean".Equals(code))
						{
							prs = "modelerType";
							code = (string)mBeanServer.getAttribute(oname, prs);
						}
						if (attribute != null)
						{
							prs = attribute;
							attributeinfo = mBeanServer.getAttribute(oname, prs);
						}
					}
					catch (javax.management.AttributeNotFoundException e)
					{
						// If the modelerType attribute was not found, the class name is used
						// instead.
						LOG.error("getting attribute " + prs + " of " + oname + " threw an exception", e);
					}
					catch (javax.management.MBeanException e)
					{
						// The code inside the attribute getter threw an exception so log it,
						// and fall back on the class name
						LOG.error("getting attribute " + prs + " of " + oname + " threw an exception", e);
					}
					catch (System.Exception e)
					{
						// For some reason even with an MBeanException available to them
						// Runtime exceptionscan still find their way through, so treat them
						// the same as MBeanException
						LOG.error("getting attribute " + prs + " of " + oname + " threw an exception", e);
					}
					catch (javax.management.ReflectionException e)
					{
						// This happens when the code inside the JMX bean (setter?? from the
						// java docs) threw an exception, so log it and fall back on the 
						// class name
						LOG.error("getting attribute " + prs + " of " + oname + " threw an exception", e);
					}
				}
				catch (javax.management.InstanceNotFoundException)
				{
					//Ignored for some reason the bean was not found so don't output it
					continue;
				}
				catch (javax.management.IntrospectionException e)
				{
					// This is an internal error, something odd happened with reflection so
					// log it and don't output the bean.
					LOG.error("Problem while trying to process JMX query: " + qry + " with MBean " + 
						oname, e);
					continue;
				}
				catch (javax.management.ReflectionException e)
				{
					// This happens when the code inside the JMX bean threw an exception, so
					// log it and don't output the bean.
					LOG.error("Problem while trying to process JMX query: " + qry + " with MBean " + 
						oname, e);
					continue;
				}
				jg.writeStartObject();
				jg.writeStringField("name", oname.ToString());
				jg.writeStringField("modelerType", code);
				if ((attribute != null) && (attributeinfo == null))
				{
					jg.writeStringField("result", "ERROR");
					jg.writeStringField("message", "No attribute with name " + attribute + " was found."
						);
					jg.writeEndObject();
					jg.writeEndArray();
					jg.close();
					response.setStatus(javax.servlet.http.HttpServletResponse.SC_NOT_FOUND);
					return;
				}
				if (attribute != null)
				{
					writeAttribute(jg, attribute, attributeinfo);
				}
				else
				{
					javax.management.MBeanAttributeInfo[] attrs = minfo.getAttributes();
					for (int i = 0; i < attrs.Length; i++)
					{
						writeAttribute(jg, oname, attrs[i]);
					}
				}
				jg.writeEndObject();
			}
			jg.writeEndArray();
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeAttribute(org.codehaus.jackson.JsonGenerator jg, javax.management.ObjectName
			 oname, javax.management.MBeanAttributeInfo attr)
		{
			if (!attr.isReadable())
			{
				return;
			}
			string attName = attr.getName();
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
				value = mBeanServer.getAttribute(oname, attName);
			}
			catch (javax.management.RuntimeMBeanException e)
			{
				// UnsupportedOperationExceptions happen in the normal course of business,
				// so no need to log them as errors all the time.
				if (e.InnerException is System.NotSupportedException)
				{
					LOG.debug("getting attribute " + attName + " of " + oname + " threw an exception"
						, e);
				}
				else
				{
					LOG.error("getting attribute " + attName + " of " + oname + " threw an exception"
						, e);
				}
				return;
			}
			catch (javax.management.RuntimeErrorException e)
			{
				// RuntimeErrorException happens when an unexpected failure occurs in getAttribute
				// for example https://issues.apache.org/jira/browse/DAEMON-120
				LOG.debug("getting attribute " + attName + " of " + oname + " threw an exception"
					, e);
				return;
			}
			catch (javax.management.AttributeNotFoundException)
			{
				//Ignored the attribute was not found, which should never happen because the bean
				//just told us that it has this attribute, but if this happens just don't output
				//the attribute.
				return;
			}
			catch (javax.management.MBeanException e)
			{
				//The code inside the attribute getter threw an exception so log it, and
				// skip outputting the attribute
				LOG.error("getting attribute " + attName + " of " + oname + " threw an exception"
					, e);
				return;
			}
			catch (System.Exception e)
			{
				//For some reason even with an MBeanException available to them Runtime exceptions
				//can still find their way through, so treat them the same as MBeanException
				LOG.error("getting attribute " + attName + " of " + oname + " threw an exception"
					, e);
				return;
			}
			catch (javax.management.ReflectionException e)
			{
				//This happens when the code inside the JMX bean (setter?? from the java docs)
				//threw an exception, so log it and skip outputting the attribute
				LOG.error("getting attribute " + attName + " of " + oname + " threw an exception"
					, e);
				return;
			}
			catch (javax.management.InstanceNotFoundException)
			{
				//Ignored the mbean itself was not found, which should never happen because we
				//just accessed it (perhaps something unregistered in-between) but if this
				//happens just don't output the attribute.
				return;
			}
			writeAttribute(jg, attName, value);
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeAttribute(org.codehaus.jackson.JsonGenerator jg, string attName
			, object value)
		{
			jg.writeFieldName(attName);
			writeObject(jg, value);
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeObject(org.codehaus.jackson.JsonGenerator jg, object value)
		{
			if (value == null)
			{
				jg.writeNull();
			}
			else
			{
				java.lang.Class c = Sharpen.Runtime.getClassForObject(value);
				if (c.isArray())
				{
					jg.writeStartArray();
					int len = java.lang.reflect.Array.getLength(value);
					for (int j = 0; j < len; j++)
					{
						object item = java.lang.reflect.Array.get(value, j);
						writeObject(jg, item);
					}
					jg.writeEndArray();
				}
				else
				{
					if (value is java.lang.Number)
					{
						java.lang.Number n = (java.lang.Number)value;
						jg.writeNumber(n.ToString());
					}
					else
					{
						if (value is bool)
						{
							bool b = (bool)value;
							jg.writeBoolean(b);
						}
						else
						{
							if (value is javax.management.openmbean.CompositeData)
							{
								javax.management.openmbean.CompositeData cds = (javax.management.openmbean.CompositeData
									)value;
								javax.management.openmbean.CompositeType comp = cds.getCompositeType();
								System.Collections.Generic.ICollection<string> keys = comp.keySet();
								jg.writeStartObject();
								foreach (string key in keys)
								{
									writeAttribute(jg, key, cds.get(key));
								}
								jg.writeEndObject();
							}
							else
							{
								if (value is javax.management.openmbean.TabularData)
								{
									javax.management.openmbean.TabularData tds = (javax.management.openmbean.TabularData
										)value;
									jg.writeStartArray();
									foreach (object entry in tds.values())
									{
										writeObject(jg, entry);
									}
									jg.writeEndArray();
								}
								else
								{
									jg.writeString(value.ToString());
								}
							}
						}
					}
				}
			}
		}
	}
}
