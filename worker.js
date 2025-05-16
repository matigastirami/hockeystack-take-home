const hubspot = require('@hubspot/api-client');
const { queue } = require('async');
const _ = require('lodash');

const { filterNullValuesFromObject, goal } = require('./utils');
const Domain = require('./Domain');

const hubspotClient = new hubspot.Client({ accessToken: '' });
const propertyPrefix = 'hubspot__';
let expirationDate;

const generateLastModifiedDateFilter = (date, nowDate, propertyName = 'hs_lastmodifieddate') => {
  const lastModifiedDateFilter = date ?
    {
      filters: [
        { propertyName, operator: 'GTQ', value: `${date.valueOf()}` },
        { propertyName, operator: 'LTQ', value: `${nowDate.valueOf()}` }
      ]
    } :
    {};

  return lastModifiedDateFilter;
};

const saveDomain = async domain => {
  // disable this for testing purposes
  return;

  domain.markModified('integrations.hubspot.accounts');
  await domain.save();
};

/**
 * Get access token from HubSpot
 */
const refreshAccessToken = async (domain, hubId, tryCount) => {
  const { HUBSPOT_CID, HUBSPOT_CS } = process.env;
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const { accessToken, refreshToken } = account;

  return hubspotClient.oauth.tokensApi
    .createToken('refresh_token', undefined, undefined, HUBSPOT_CID, HUBSPOT_CS, refreshToken)
    .then(async result => {
      const body = result.body ? result.body : result;

      const newAccessToken = body.accessToken;
      expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());

      hubspotClient.setAccessToken(newAccessToken);
      if (newAccessToken !== accessToken) {
        account.accessToken = newAccessToken;
      }

      return true;
    });
};

/**
 * Get recently modified companies as 100 companies per page
 */
const processCompanies = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.companies);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now);
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'name',
        'domain',
        'country',
        'industry',
        'description',
        'annualrevenue',
        'numberofemployees',
        'hs_lead_status'
      ],
      limit,
      after: offsetObject.after
    };

    let searchResult = {};

    let tryCount = 0;
    while (tryCount <= 4) {
      try {
        searchResult = await hubspotClient.crm.companies.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) throw new Error('Failed to fetch companies for the 4th time. Aborting.');

    const data = searchResult?.results || [];
    offsetObject.after = parseInt(searchResult?.paging?.next?.after);

    console.log('fetch company batch');

    data.forEach(company => {
      if (!company.properties) return;

      const actionTemplate = {
        includeInAnalytics: 0,
        companyProperties: {
          company_id: company.id,
          company_domain: company.properties.domain,
          company_industry: company.properties.industry
        }
      };

      const isCreated = !lastPulledDate || (new Date(company.createdAt) > lastPulledDate);

      q.push({
        actionName: isCreated ? 'Company Created' : 'Company Updated',
        actionDate: new Date(isCreated ? company.createdAt : company.updatedAt) - 2000,
        ...actionTemplate
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.companies = now;
  await saveDomain(domain);

  return true;
};

/**
 * Get recently modified contacts as 100 contacts per page
 */
const processContacts = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.contacts);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, 'lastmodifieddate');
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'firstname',
        'lastname',
        'jobtitle',
        'email',
        'hubspotscore',
        'hs_lead_status',
        'hs_analytics_source',
        'hs_latest_source'
      ],
      limit,
      after: offsetObject.after
    };

    let searchResult = {};

    let tryCount = 0;
    while (tryCount <= 4) {
      try {
        searchResult = await hubspotClient.crm.contacts.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) throw new Error('Failed to fetch contacts for the 4th time. Aborting.');

    const data = searchResult.results || [];

    console.log('fetch contact batch');

    offsetObject.after = parseInt(searchResult.paging?.next?.after);
    const contactIds = data.map(contact => contact.id);

    // contact to company association
    const contactsToAssociate = contactIds;
    const companyAssociationsResults = (await (await hubspotClient.apiRequest({
      method: 'post',
      path: '/crm/v3/associations/CONTACTS/COMPANIES/batch/read',
      body: { inputs: contactsToAssociate.map(contactId => ({ id: contactId })) }
    })).json())?.results || [];

    const companyAssociations = Object.fromEntries(companyAssociationsResults.map(a => {
      if (a.from) {
        contactsToAssociate.splice(contactsToAssociate.indexOf(a.from.id), 1);
        return [a.from.id, a.to[0].id];
      } else return false;
    }).filter(x => x));

    data.forEach(contact => {
      if (!contact.properties || !contact.properties.email) return;

      const companyId = companyAssociations[contact.id];

      const isCreated = new Date(contact.createdAt) > lastPulledDate;

      const userProperties = {
        company_id: companyId,
        contact_name: ((contact.properties.firstname || '') + ' ' + (contact.properties.lastname || '')).trim(),
        contact_title: contact.properties.jobtitle,
        contact_source: contact.properties.hs_analytics_source,
        contact_status: contact.properties.hs_lead_status,
        contact_score: parseInt(contact.properties.hubspotscore) || 0
      };

      const actionTemplate = {
        includeInAnalytics: 0,
        identity: contact.properties.email,
        userProperties: filterNullValuesFromObject(userProperties)
      };

      q.push({
        actionName: isCreated ? 'Contact Created' : 'Contact Updated',
        actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
        ...actionTemplate
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.contacts = now;
  await saveDomain(domain);

  return true;
};

const processMeetings = async(domain, hubId, q) => {
  console.log(`[meetings] Start processing meetings for hubId=${hubId}`);
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = account.lastPulledDates.meetings ? new Date(account.lastPulledDates.meetings) : null;
  const now = new Date();
  let offsetObject = {};
  let hasMore = true;
  let processedCount = 0;
  let skippedCount = 0;

  try {
    while (hasMore) {
      const searchResult = await hubspotClient.crm.objects.meetings.searchApi.doSearch({
        filterGroups: [
          {
            filters: [
              {
                propertyName: 'hs_lastmodifieddate',
                operator: 'GT',
                value: String(lastPulledDate.valueOf()),
              },
              {
                propertyName: 'hs_lastmodifieddate',
                operator: 'LT',
                value: String(now.valueOf()),
              }
            ]
          }
        ],
        sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
        limit: 100,
        after: offsetObject.after,
        properties: ['hs_meeting_title', 'hs_meeting_start_time'],
      });

      const data = searchResult.results;

      console.log(`[meetings] Fetched ${data.length} meetings | after=${offsetObject.after || 'initial'}`);

      if (!data.length) {
        console.warn(`Could not find more meetings data.`)
        hasMore = false;
        break;
      }

      for (let meeting of data) {
        const attendeeEmails = await getAttendeeEmailsForMeeting(meeting.id);
        if (!attendeeEmails.length) {
          console.warn(`[meetings] Skipping meeting ${meeting.id} — no attendees found`);
          skippedCount++;
          continue;
        }

        console.log(`[meetings] Meeting ${meeting.id} attendees: ${attendeeEmails.join(', ')}`);

        if (!meeting.properties.hs_meeting_start_time) {
          console.warn(`[meetings] Skipping meeting ${meeting.id} — missing hs_timestamp`);
          skippedCount++;
          continue;
        }

        const isCreated = new Date(meeting.createdAt) > lastPulledDate;
        const actionName = isCreated ? 'Meeting Created' : 'Meeting Updated';
        const actionDate = new Date(isCreated ? meeting.createdAt : meeting.updatedAt);
        
        for (const email of attendeeEmails) {
          // Pushing action with identity = email because I think it can allow analytics systems 
          // to associate the action with an user or email but maybe I'm wrong due to I'm missing more context
          q.push({
            actionName,
            actionDate: new Date(actionDate - 2000),
            includeInAnalytics: 0,
            identity: email,
            userProperties: {
              meeting_id: meeting.id,
              meeting_title: meeting.properties.hs_meeting_title || `Meeting ${meeting.id}`,
              meeting_timestamp: meeting.properties.hs_meeting_start_time,
            }
          });
          processedCount++;
          console.log(`[meetings] Queued ${attendeeEmails.length} actions for meeting ${meeting.id}`);
        }
      }

      offsetObject.after = searchResult?.paging?.next?.after;

      if (!offsetObject.after) {
        hasMore = false;
      } else if (offsetObject.after >= 9900) {
        offsetObject.after = 0;
        offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
      }
    }

    account.lastPulledDates.meetings = now;
    await saveDomain(domain);

    console.log(`[meetings] Finished processing. Total meetings processed: ${processedCount}, skipped: ${skippedCount}`);
    return true;
  } catch (e) {
    console.error(e, {})
  }
}

const getAttendeeEmailsForMeeting = async (meetingId) => {
  try {
    console.log(`[meetings] fetching associations and contacts for meetingId ${meetingId}`);
    const associations = await hubspotClient.crm.objects.meetings.associationsApi.getAll(meetingId, 'contacts');
    const contactIds = associations.results.map(c => c.toObjectId).filter(Boolean);

    if (!contactIds.length) return [];

    const batchInput = {
      properties: ['email'],
      inputs: contactIds.map(id => ({id}))
    }

    const contactResults = await hubspotClient.crm.contacts.batchApi.read(batchInput);
    return contactResults.results
      .map(c => c.properties?.email)
      .filter(Boolean);
  } catch(err) {
      console.error(`[meetings] Failed to fetch associations for meeting ${meetingId}`, {
        error: err.message,
        response: err.response?.body || null
      });
      return [];
  }
}

const createQueue = (domain, actions) => queue(async (action, callback) => {
  actions.push(action);

  if (actions.length > 2000) {
    console.log('inserting actions to database', { apiKey: domain.apiKey, count: actions.length });

    const copyOfActions = _.cloneDeep(actions);
    actions.splice(0, actions.length);

    goal(copyOfActions);
  }

  callback();
}, 100000000);

const drainQueue = async (domain, actions, q) => {
  if (q.length() > 0) await q.drain();

  if (actions.length > 0) {
    goal(actions)
  }

  return true;
};

const pullDataFromHubspot = async () => {
  console.log('start pulling data from HubSpot');

  const domain = await Domain.findOne({});

  for (const account of domain.integrations.hubspot.accounts) {
    console.log('start processing account');

    try {
      await refreshAccessToken(domain, account.hubId);
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'refreshAccessToken' } });
    }

    const actions = [];
    const q = createQueue(domain, actions);

    // try {
    //   await processContacts(domain, account.hubId, q);
    //   console.log('process contacts');
    // } catch (err) {
    //   console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processContacts', hubId: account.hubId } });
    // }

    // try {
    //   await processCompanies(domain, account.hubId, q);
    //   console.log('process companies');
    // } catch (err) {
    //   console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processCompanies', hubId: account.hubId } });
    // }
    
    try {
      await processMeetings(domain, account.hubId, q);
      console.log('meetings processed');
    } catch(err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processMeetings', hubId: account.hubId } });
    }

    try {
      await drainQueue(domain, actions, q);
      console.log('drain queue');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'drainQueue', hubId: account.hubId } });
    }

    await saveDomain(domain);

    console.log('finish processing account');
  }

  process.exit();
};

module.exports = pullDataFromHubspot;
