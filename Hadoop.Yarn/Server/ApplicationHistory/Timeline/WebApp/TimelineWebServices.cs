using System;
using System.Collections.Generic;
using System.IO;
using Javax.Servlet.Http;
using Javax.WS.RS;
using Javax.WS.RS.Core;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Webapp
{
	public class TimelineWebServices
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Timeline.Webapp.TimelineWebServices
			));

		private TimelineDataManager timelineDataManager;

		[Com.Google.Inject.Inject]
		public TimelineWebServices(TimelineDataManager timelineDataManager)
		{
			//TODO: support XML serialization/deserialization
			this.timelineDataManager = timelineDataManager;
		}

		public class AboutInfo
		{
			private string about;

			public AboutInfo()
			{
			}

			public AboutInfo(string about)
			{
				this.about = about;
			}

			public virtual string GetAbout()
			{
				return about;
			}

			public virtual void SetAbout(string about)
			{
				this.about = about;
			}
		}

		/// <summary>Return the description of the timeline web services.</summary>
		[GET]
		public virtual TimelineWebServices.AboutInfo About(HttpServletRequest req, HttpServletResponse
			 res)
		{
			/* , MediaType.APPLICATION_XML */
			Init(res);
			return new TimelineWebServices.AboutInfo("Timeline API");
		}

		/// <summary>Return a list of entities that match the given parameters.</summary>
		[GET]
		public virtual TimelineEntities GetEntities(HttpServletRequest req, HttpServletResponse
			 res, string entityType, string primaryFilter, string secondaryFilter, string windowStart
			, string windowEnd, string fromId, string fromTs, string limit, string fields)
		{
			/* , MediaType.APPLICATION_XML */
			Init(res);
			try
			{
				return timelineDataManager.GetEntities(ParseStr(entityType), ParsePairStr(primaryFilter
					, ":"), ParsePairsStr(secondaryFilter, ",", ":"), ParseLongStr(windowStart), ParseLongStr
					(windowEnd), ParseStr(fromId), ParseLongStr(fromTs), ParseLongStr(limit), ParseFieldsStr
					(fields, ","), GetUser(req));
			}
			catch (FormatException)
			{
				throw new BadRequestException("windowStart, windowEnd or limit is not a numeric value."
					);
			}
			catch (ArgumentException)
			{
				throw new BadRequestException("requested invalid field.");
			}
			catch (Exception e)
			{
				Log.Error("Error getting entities", e);
				throw new WebApplicationException(e, Response.Status.InternalServerError);
			}
		}

		/// <summary>Return a single entity of the given entity type and Id.</summary>
		[GET]
		public virtual TimelineEntity GetEntity(HttpServletRequest req, HttpServletResponse
			 res, string entityType, string entityId, string fields)
		{
			/* , MediaType.APPLICATION_XML */
			Init(res);
			TimelineEntity entity = null;
			try
			{
				entity = timelineDataManager.GetEntity(ParseStr(entityType), ParseStr(entityId), 
					ParseFieldsStr(fields, ","), GetUser(req));
			}
			catch (ArgumentException)
			{
				throw new BadRequestException("requested invalid field.");
			}
			catch (Exception e)
			{
				Log.Error("Error getting entity", e);
				throw new WebApplicationException(e, Response.Status.InternalServerError);
			}
			if (entity == null)
			{
				throw new NotFoundException("Timeline entity " + new EntityIdentifier(ParseStr(entityId
					), ParseStr(entityType)) + " is not found");
			}
			return entity;
		}

		/// <summary>Return the events that match the given parameters.</summary>
		[GET]
		public virtual TimelineEvents GetEvents(HttpServletRequest req, HttpServletResponse
			 res, string entityType, string entityId, string eventType, string windowStart, 
			string windowEnd, string limit)
		{
			/* , MediaType.APPLICATION_XML */
			Init(res);
			try
			{
				return timelineDataManager.GetEvents(ParseStr(entityType), ParseArrayStr(entityId
					, ","), ParseArrayStr(eventType, ","), ParseLongStr(windowStart), ParseLongStr(windowEnd
					), ParseLongStr(limit), GetUser(req));
			}
			catch (FormatException)
			{
				throw new BadRequestException("windowStart, windowEnd or limit is not a numeric value."
					);
			}
			catch (Exception e)
			{
				Log.Error("Error getting entity timelines", e);
				throw new WebApplicationException(e, Response.Status.InternalServerError);
			}
		}

		/// <summary>
		/// Store the given entities into the timeline store, and return the errors
		/// that happen during storing.
		/// </summary>
		[POST]
		public virtual TimelinePutResponse PostEntities(HttpServletRequest req, HttpServletResponse
			 res, TimelineEntities entities)
		{
			/* , MediaType.APPLICATION_XML */
			Init(res);
			UserGroupInformation callerUGI = GetUser(req);
			if (callerUGI == null)
			{
				string msg = "The owner of the posted timeline entities is not set";
				Log.Error(msg);
				throw new ForbiddenException(msg);
			}
			try
			{
				return timelineDataManager.PostEntities(entities, callerUGI);
			}
			catch (Exception e)
			{
				Log.Error("Error putting entities", e);
				throw new WebApplicationException(e, Response.Status.InternalServerError);
			}
		}

		/// <summary>
		/// Store the given domain into the timeline store, and return the errors
		/// that happen during storing.
		/// </summary>
		[PUT]
		public virtual TimelinePutResponse PutDomain(HttpServletRequest req, HttpServletResponse
			 res, TimelineDomain domain)
		{
			/* , MediaType.APPLICATION_XML */
			Init(res);
			UserGroupInformation callerUGI = GetUser(req);
			if (callerUGI == null)
			{
				string msg = "The owner of the posted timeline domain is not set";
				Log.Error(msg);
				throw new ForbiddenException(msg);
			}
			domain.SetOwner(callerUGI.GetShortUserName());
			try
			{
				timelineDataManager.PutDomain(domain, callerUGI);
			}
			catch (YarnException e)
			{
				// The user doesn't have the access to override the existing domain.
				Log.Error(e.Message, e);
				throw new ForbiddenException(e);
			}
			catch (IOException e)
			{
				Log.Error("Error putting domain", e);
				throw new WebApplicationException(e, Response.Status.InternalServerError);
			}
			return new TimelinePutResponse();
		}

		/// <summary>Return a single domain of the given domain Id.</summary>
		[GET]
		public virtual TimelineDomain GetDomain(HttpServletRequest req, HttpServletResponse
			 res, string domainId)
		{
			/* , MediaType.APPLICATION_XML */
			Init(res);
			domainId = ParseStr(domainId);
			if (domainId == null || domainId.Length == 0)
			{
				throw new BadRequestException("Domain ID is not specified.");
			}
			TimelineDomain domain = null;
			try
			{
				domain = timelineDataManager.GetDomain(ParseStr(domainId), GetUser(req));
			}
			catch (Exception e)
			{
				Log.Error("Error getting domain", e);
				throw new WebApplicationException(e, Response.Status.InternalServerError);
			}
			if (domain == null)
			{
				throw new NotFoundException("Timeline domain [" + domainId + "] is not found");
			}
			return domain;
		}

		/// <summary>Return a list of domains of the given owner.</summary>
		[GET]
		public virtual TimelineDomains GetDomains(HttpServletRequest req, HttpServletResponse
			 res, string owner)
		{
			/* , MediaType.APPLICATION_XML */
			Init(res);
			owner = ParseStr(owner);
			UserGroupInformation callerUGI = GetUser(req);
			if (owner == null || owner.Length == 0)
			{
				if (callerUGI == null)
				{
					throw new BadRequestException("Domain owner is not specified.");
				}
				else
				{
					// By default it's going to list the caller's domains
					owner = callerUGI.GetShortUserName();
				}
			}
			try
			{
				return timelineDataManager.GetDomains(owner, callerUGI);
			}
			catch (Exception e)
			{
				Log.Error("Error getting domains", e);
				throw new WebApplicationException(e, Response.Status.InternalServerError);
			}
		}

		private void Init(HttpServletResponse response)
		{
			response.SetContentType(null);
		}

		private static UserGroupInformation GetUser(HttpServletRequest req)
		{
			string remoteUser = req.GetRemoteUser();
			UserGroupInformation callerUGI = null;
			if (remoteUser != null)
			{
				callerUGI = UserGroupInformation.CreateRemoteUser(remoteUser);
			}
			return callerUGI;
		}

		private static ICollection<string> ParseArrayStr(string str, string delimiter)
		{
			if (str == null)
			{
				return null;
			}
			ICollection<string> strSet = new TreeSet<string>();
			string[] strs = str.Split(delimiter);
			foreach (string aStr in strs)
			{
				strSet.AddItem(aStr.Trim());
			}
			return strSet;
		}

		private static NameValuePair ParsePairStr(string str, string delimiter)
		{
			if (str == null)
			{
				return null;
			}
			string[] strs = str.Split(delimiter, 2);
			try
			{
				return new NameValuePair(strs[0].Trim(), GenericObjectMapper.ObjectReader.ReadValue
					(strs[1].Trim()));
			}
			catch (Exception)
			{
				// didn't work as an Object, keep it as a String
				return new NameValuePair(strs[0].Trim(), strs[1].Trim());
			}
		}

		private static ICollection<NameValuePair> ParsePairsStr(string str, string aDelimiter
			, string pDelimiter)
		{
			if (str == null)
			{
				return null;
			}
			string[] strs = str.Split(aDelimiter);
			ICollection<NameValuePair> pairs = new HashSet<NameValuePair>();
			foreach (string aStr in strs)
			{
				pairs.AddItem(ParsePairStr(aStr, pDelimiter));
			}
			return pairs;
		}

		private static EnumSet<TimelineReader.Field> ParseFieldsStr(string str, string delimiter
			)
		{
			if (str == null)
			{
				return null;
			}
			string[] strs = str.Split(delimiter);
			IList<TimelineReader.Field> fieldList = new AList<TimelineReader.Field>();
			foreach (string s in strs)
			{
				s = StringUtils.ToUpperCase(s.Trim());
				if (s.Equals("EVENTS"))
				{
					fieldList.AddItem(TimelineReader.Field.Events);
				}
				else
				{
					if (s.Equals("LASTEVENTONLY"))
					{
						fieldList.AddItem(TimelineReader.Field.LastEventOnly);
					}
					else
					{
						if (s.Equals("RELATEDENTITIES"))
						{
							fieldList.AddItem(TimelineReader.Field.RelatedEntities);
						}
						else
						{
							if (s.Equals("PRIMARYFILTERS"))
							{
								fieldList.AddItem(TimelineReader.Field.PrimaryFilters);
							}
							else
							{
								if (s.Equals("OTHERINFO"))
								{
									fieldList.AddItem(TimelineReader.Field.OtherInfo);
								}
								else
								{
									throw new ArgumentException("Requested nonexistent field " + s);
								}
							}
						}
					}
				}
			}
			if (fieldList.Count == 0)
			{
				return null;
			}
			TimelineReader.Field f1 = fieldList.Remove(fieldList.Count - 1);
			if (fieldList.Count == 0)
			{
				return EnumSet.Of(f1);
			}
			else
			{
				return EnumSet.Of(f1, Sharpen.Collections.ToArray(fieldList, new TimelineReader.Field
					[fieldList.Count]));
			}
		}

		private static long ParseLongStr(string str)
		{
			return str == null ? null : long.Parse(str.Trim());
		}

		private static string ParseStr(string str)
		{
			return str == null ? null : str.Trim();
		}
	}
}
