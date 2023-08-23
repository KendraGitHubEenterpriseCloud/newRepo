package com.amazonaws.kendra.connector.confluence.client;

import static com.amazonaws.kendra.connector.confluence.utils.Constants.AUTHENTICATION_TYPE;
import static com.amazonaws.kendra.connector.confluence.utils.Constants.COMMA;
import static com.amazonaws.kendra.connector.confluence.utils.Constants.CONFLUENCE_ENTITY_TYPE;
import static com.amazonaws.kendra.connector.confluence.utils.Constants.CONFLUENCE_TYPE;
import static com.amazonaws.kendra.connector.confluence.utils.Constants.HOST_URL;
import static com.amazonaws.kendra.connector.confluence.utils.Constants.OAUTH2;
import static com.amazonaws.kendra.connector.confluence.utils.Constants.SAAS;
import static com.amazonaws.kendra.connector.confluence.utils.Constants.USER_NAME;

import com.amazonaws.kendra.connector.confluence.configuration.ConfluenceConfiguration;
import com.amazonaws.kendra.connector.confluence.model.Attachment;
import com.amazonaws.kendra.connector.confluence.model.Blog;
import com.amazonaws.kendra.connector.confluence.model.Comment;
import com.amazonaws.kendra.connector.confluence.model.Label;
import com.amazonaws.kendra.connector.confluence.model.LabelList;
import com.amazonaws.kendra.connector.confluence.model.Page;
import com.amazonaws.kendra.connector.confluence.model.Space;
import com.amazonaws.kendra.connector.confluence.model.item.AttachmentItem;
import com.amazonaws.kendra.connector.confluence.model.item.BlogItem;
import com.amazonaws.kendra.connector.confluence.model.item.CommentItem;
import com.amazonaws.kendra.connector.confluence.model.item.PageItem;
import com.amazonaws.kendra.connector.confluence.model.item.SpaceItem;
import com.amazonaws.kendra.connector.confluence.service.ConfluenceConfigValidatorService;
import com.amazonaws.kendra.connector.confluence.service.ConfluenceService;
import com.amazonaws.kendra.connector.confluence.token.ConfluenceChangeLogToken;
import com.amazonaws.kendra.connector.confluence.token.ConfluenceChangeLogTokenSerializer;
import com.amazonaws.kendra.connector.confluence.utils.CommonUtil;
import com.amazonaws.kendra.connector.confluence.utils.ConfluenceContentType;
import com.amazonaws.kendra.connector.confluence.utils.ItemInfoBuilder;
import com.amazonaws.kendra.connector.confluence.utils.StackTraceLogger;
import com.amazonaws.kendra.connector.sdk.client.RepositoryClient;
import com.amazonaws.kendra.connector.sdk.exception.KendraConnectorException;
import com.amazonaws.kendra.connector.sdk.log.DocumentId;
import com.amazonaws.kendra.connector.sdk.log.MaskedJson;
import com.amazonaws.kendra.connector.sdk.manager.RepositoryStateManager;
import com.amazonaws.kendra.connector.sdk.model.ConnectionStatus;
import com.amazonaws.kendra.connector.sdk.model.item.Item;
import com.amazonaws.kendra.connector.sdk.model.item.ItemInfo;
import com.amazonaws.kendra.connector.sdk.model.repository.RepositoryConfiguration;
import com.amazonaws.services.kendra.model.Principal;
import com.amazonaws.util.CollectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.CustomLog;
import lombok.NoArgsConstructor;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;

/**
 * Confluence Client.
 */
@CustomLog
@NoArgsConstructor
public class ConfluenceClient implements RepositoryClient {
  private static final Logger appLog =
      org.slf4j.LoggerFactory.getLogger(ConfluenceClient.class);
  private ConfluenceConfiguration confluenceConfiguration;
  private long changeLogIteratorInitTimestamp = 0;
  private int maxResultSize = 198;
  private ConfluenceChangeLogTokenSerializer serializer;
  private RepositoryStateManager repositoryStateManager;
  private ConfluenceFullCrawlIterator confluenceFullCrawlIterator;
  private ConfluenceService confluenceService;
  private ConfluenceConfigValidatorService confluenceConfigValidatorService;

  @Override
  public ConnectionStatus testConnection() {
    try {
      JSONObject connectionObject = new JSONObject()
          .put(AUTHENTICATION_TYPE, confluenceConfiguration.getOauthType())
          .put(USER_NAME, confluenceConfiguration.getConfluenceUserName())
          .put(CONFLUENCE_TYPE, confluenceConfiguration.getConfluenceInstanceType())
          .put(HOST_URL, confluenceConfiguration.getUrl());
      log.info(new MaskedJson(connectionObject.toString()),
          "[{}] - Testing connection with following Confluence details",
          StackTraceLogger.getUuid());
      if (confluenceConfiguration.getConfluenceInstanceType().equals(SAAS)
          && confluenceConfiguration.getOauthType().equals(OAUTH2)) {
        ConfluenceService.setBaseUrl(CommonUtil.buildUrlForOauth(this.confluenceConfiguration));
      }
      confluenceService.reset();
      confluenceService.testConnection(confluenceConfiguration);
      log.info("[{}] - Connection successfully established.", StackTraceLogger.getUuid());

      return ConnectionStatus.builder().isConnected(true).build();
    } catch (KendraConnectorException | JSONException kce) {
      StackTraceLogger.logStackTrace(kce, appLog, log);
      log.error("[{}] - Connection could not be established: {}",
          StackTraceLogger.getUuid(), kce.getMessage(), kce);

      return ConnectionStatus.builder().isConnected(false).statusMessage(kce.getMessage()).build();
    }
  }

  @Override
  public void resetAndInitialize(RepositoryConfiguration repositoryConfiguration) {
    try {
      log.info("[{}] - Resetting and Initializing the state for Confluence connector.",
          StackTraceLogger.getUuid());
      serializer = ConfluenceChangeLogTokenSerializer.builder()
          .objectMapper(new ObjectMapper()).build();
      this.confluenceConfiguration = ConfluenceConfiguration.of(repositoryConfiguration);
      this.confluenceConfigValidatorService = new ConfluenceConfigValidatorService();
      //Validate config
      confluenceConfigValidatorService.validateConfiguration(confluenceConfiguration);
      this.confluenceService = new ConfluenceService(confluenceConfiguration);
      this.confluenceFullCrawlIterator =
          new ConfluenceFullCrawlIterator(this.confluenceService, this.confluenceConfiguration);
    } catch (Exception e) {
      StackTraceLogger.logStackTrace(e, appLog, log);
      log.error("[{}] - An exception has occurred in the Confluence connector configuration:"
          + " {}", StackTraceLogger.getUuid(), e.getMessage(), e);
      log.error("An exception has occurred in the Confluence connector configuration :: ", e);
      throw e;
    }
  }

  @Override
  public Optional<Item> getItem(ItemInfo itemInfo) {
    Optional<Item> resultItem = Optional.empty();
    List<Principal> principals = new ArrayList<>();
    final ConfluenceContentType confluenceContentType =
        ConfluenceContentType.valueOf(itemInfo.getMetadata()
            .get(CONFLUENCE_ENTITY_TYPE));
    log.info("[{}]- Fetching metadata for document id: {} of {} entity.",
        StackTraceLogger.getUuid(), itemInfo.getItemId(), confluenceContentType);

    switch (confluenceContentType) {
      case SPACE:
        Space space = confluenceService.getSpaceDetails(itemInfo.getItemId(), 0);
        if (Objects.nonNull(space)) {
          if (confluenceConfiguration.getConfluenceInstanceType().equals(SAAS)) {
            principals = confluenceService.getSpaceAcls(space.getKey());
          }
          resultItem = Optional.ofNullable(
              ItemInfoBuilder.buildSpaceItem(space, itemInfo, confluenceConfiguration,
                  principals.size() == 0 ? null : principals));
          if (resultItem.isPresent()) {
            SpaceItem spaceItem = (SpaceItem) resultItem.get();
            log.info(new DocumentId(spaceItem.getDocumentId(), spaceItem.getSpaceKey(),
                    spaceItem.getUrl()), "[{}] - [{}] Metadata retrieved successfully "
                    + "for Space entity.", StackTraceLogger.getUuid(),
                spaceItem.getDocumentId());
          }
        } else {
          log.info("[{}] - Empty API response for the Space id : {}, "
                  + "hence skipping the document from ingestion. Metadata : {}",
              StackTraceLogger.getUuid(), itemInfo.getItemId(), itemInfo.getMetadata());
        }
        break;
      case BLOGPOST:
        Blog blogPost = confluenceService.getBlogPostDetails(itemInfo.getItemId(), 0);
        if (Objects.nonNull(blogPost)) {
          if (Objects.nonNull(blogPost.getMetadata().getLabelList())
              && blogPost.getMetadata().getLabelList().getResults().size() > maxResultSize) {
            blogPost.setLabelList(getLabels(itemInfo.getItemId()));
          } else {
            blogPost.setLabelList(createLabel(blogPost.getMetadata().getLabelList()));
          }
          principals = confluenceService
              .getRestrictions(blogPost.getId(), blogPost.getSpace(),
                  confluenceConfiguration,
                  blogPost.getRestrictions(), blogPost.getAncestors());
          resultItem = Optional.ofNullable(
              ItemInfoBuilder.buildBlogPostItem(blogPost, itemInfo, confluenceConfiguration,
                  principals.size() == 0 ? null : principals));
          if (resultItem.isPresent()) {
            BlogItem blogItem = (BlogItem) resultItem.get();
            log.info(new DocumentId(blogItem.getDocumentId(), blogItem.getSpaceKey(),
                    blogItem.getUrl()), "[{}] - [{}] Metadata retrieved successfully "
                    + "for BlogPost entity.", StackTraceLogger.getUuid(),
                blogItem.getDocumentId());
          }
        } else {
          log.info("Empty API response for the BlogPost id : {}, "
                  + "hence skipping the document from ingestion. Metadata : {}",
              itemInfo.getItemId(), itemInfo.getMetadata());
        }
        break;
      case PAGE:
        Page page = confluenceService.getPageDetails(itemInfo.getItemId(), 0);
        if (Objects.nonNull(page)) {
          if (Objects.nonNull(page.getMetadata().getLabelList())
              && page.getMetadata().getLabelList().getResults().size() > maxResultSize) {
            page.setLabelList(getLabels(itemInfo.getItemId()));
          } else {
            page.setLabelList(createLabel(page.getMetadata().getLabelList()));
          }
          principals = confluenceService
              .getRestrictions(page.getId(), page.getSpace(),
                  confluenceConfiguration, page.getRestrictions(), page.getAncestors());

          resultItem = Optional.ofNullable(
              ItemInfoBuilder.buildPageItem(page, itemInfo, confluenceConfiguration,
                  principals.size() == 0 ? null : principals));

          if (resultItem.isPresent()) {
            PageItem pageItem = (PageItem) resultItem.get();
            log.info(new DocumentId(pageItem.getDocumentId(), pageItem.getSpaceKey(),
                    pageItem.getUrl()), "[{}] - [{}] Metadata retrieved successfully "
                    + "for Page entity.", StackTraceLogger.getUuid(),
                pageItem.getDocumentId());
          }
        } else {
          log.info("[{}] - Empty API response for the Page id : {}, "
                  + "hence skipping the document from ingestion. Metadata : {}",
              StackTraceLogger.getUuid(), itemInfo.getItemId(), itemInfo.getMetadata());
        }
        break;
      case COMMENT:
        Comment comment = confluenceService.getCommentDetails(itemInfo.getItemId(), 0);
        if (Objects.nonNull(comment)) {
          principals = confluenceService
              .getRestrictions(comment.getContainer().getId(),
                  comment.getSpace(),
                  confluenceConfiguration, comment.getContainer().getRestrictions(),
                  comment.getContainer().getAncestors());

          resultItem = Optional.ofNullable(
              ItemInfoBuilder.buildCommentItem(comment, itemInfo, confluenceConfiguration,
                  principals.size() == 0 ? null : principals));
          if (resultItem.isPresent()) {
            CommentItem commentItem = (CommentItem) resultItem.get();
            log.info(new DocumentId(commentItem.getDocumentId(), commentItem.getSpaceKey(),
                    commentItem.getUrl()), "[{}] - [{}] Metadata retrieved successfully "
                    + "for Comment entity.", StackTraceLogger.getUuid(),
                commentItem.getDocumentId());
          }
        } else {
          log.info("[{}] - Empty API response for the Comment id : {}, "
                  + "hence skipping the document from ingestion. Metadata : {}",
              StackTraceLogger.getUuid(), itemInfo.getItemId(), itemInfo.getMetadata());
        }
        break;
      case ATTACHMENT:
        Attachment attachment = confluenceService.getAttachmentDetails(itemInfo.getItemId(), 0);
        if (Objects.nonNull(attachment)) {
          if (Objects.nonNull(attachment.getMetadata().getLabelList())
              && attachment.getMetadata().getLabelList().getResults().size() > maxResultSize) {
            attachment.setLabelList(getLabels(itemInfo.getItemId()));
          } else {
            attachment.setLabelList(createLabel(attachment.getMetadata().getLabelList()));
          }
          principals = confluenceService
              .getRestrictions(attachment.getContainer().getId(),
                  attachment.getSpace(), confluenceConfiguration,
                  attachment.getContainer().getRestrictions(),
                  attachment.getContainer().getAncestors());

          resultItem = Optional.ofNullable(
              ItemInfoBuilder.buildAttachmentItem(confluenceService, attachment,
                  itemInfo, confluenceConfiguration, principals.size() == 0
                      ? null : principals));
          if (resultItem.isPresent()) {
            AttachmentItem attachmentItem = (AttachmentItem) resultItem.get();
            log.info(new DocumentId(attachmentItem.getDocumentId(), attachmentItem.getSpaceKey(),
                    attachmentItem.getUrl()), "[{}] - [{}] Metadata retrieved successfully "
                    + "for the Attachment entity.", StackTraceLogger.getUuid(),
                attachmentItem.getDocumentId());
          }
        } else {
          log.info("[{}] - Empty API response for Attachment id : {}, "
                  + "hence skipping the document from ingestion. Metadata : {}",
              StackTraceLogger.getUuid(), itemInfo.getItemId(), itemInfo.getMetadata());
        }
        break;
      default:
        resultItem = Optional.empty();
    }
    return resultItem;
  }

  /**
   * Method for getting labels for BlogPost, Page, Attachment.
   *
   * @param parentId - itemId
   * @return Labels
   */
  private String getLabels(String parentId) {
    LabelList labelList = confluenceService.getLabels(parentId, 0);
    return createLabel(labelList);
  }

  /**
   * Method to get label list for blog, page and attachment.
   *
   * @param labelList input param
   * @return label list
   */
  private String createLabel(LabelList labelList) {
    if (Objects.nonNull(labelList) && !CollectionUtils.isNullOrEmpty(labelList.getResults())) {
      return labelList.getResults().stream().map(Label::getName).collect(
          Collectors.joining(COMMA));
    }
    return null;
  }

  @Override
  public Iterator<ItemInfo> listItems() {
    this.confluenceFullCrawlIterator.initialize();
    this.changeLogIteratorInitTimestamp = Date.from(Instant.now()).getTime();
    return this.confluenceFullCrawlIterator;
  }

  @Override
  public Iterator<ItemInfo> listChangeLogItems(String changeLogToken) {
    return null;
  }

  @Override
  public Optional<String> getLatestChangeLogToken() {
    this.changeLogIteratorInitTimestamp =
        this.changeLogIteratorInitTimestamp != 0 ? this.changeLogIteratorInitTimestamp :
            Date.from(Instant.now()).getTime();
    ConfluenceChangeLogToken changeLogToken =
        ConfluenceChangeLogToken.builder().lastCrawlTime(changeLogIteratorInitTimestamp)
            .hostUrl(confluenceConfiguration.getUrl())
            .repositoryConfigurations(
                confluenceConfiguration.getRepositoryConfiguration().getRepositoryConfigurations())
            .additionalProperties(
                confluenceConfiguration.getRepositoryConfiguration().getAdditionalProperties())
            .build();
    return Optional.of(serializer.serialize(changeLogToken));
  }

  @Override
  public boolean isCrawlConfigurationUpdatedForChangeLog(String previousChangeLogToken) {
    return true;
  }

  /**
   * Method to set state manager.
   *
   * @param repositoryStateManager input parameter
   */
  @Override
  public void setStateManager(RepositoryStateManager repositoryStateManager) {
    this.repositoryStateManager = repositoryStateManager;
  }
}