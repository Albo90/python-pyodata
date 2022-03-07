"""OData Client Implementation"""

import logging
import warnings

import pyodata.v2.model
import pyodata.v2.async_service
from pyodata.exceptions import PyODataException, HttpError


async def _fetch_metadata(connection, url, logger):
    # download metadata
    logger.info('Fetching metadata')
    async with connection.get() as resp:
        content = await resp.read()
        logger.debug('Retrieved the response:\n%s\n%s',
                     '\n'.join((f'H: {key}: {value}' for key, value in resp.headers.items())),
                     content)

        if resp.status != 200:
            raise HttpError(
                f'Metadata request failed, status code: {resp.status}, body:\n{content}', resp)

        mime_type = resp.headers['content-type']
        if not any((typ in ['application/xml', 'application/atom+xml', 'text/xml'] for typ in mime_type.split(';'))):
            raise HttpError(
                f'Metadata request did not return XML, MIME type: {mime_type}, body:\n{content}',
                resp)

        return content


class AsyncClient:
    """OData service client"""

    # pylint: disable=too-few-public-methods

    ODATA_VERSION_2 = 2

    @staticmethod
    async def new(url, connection, odata_version=ODATA_VERSION_2, namespaces=None,
                  config: pyodata.v2.model.Config = None, metadata: str = None):
        """Create instance of the OData Client for given URL"""

        logger = logging.getLogger('pyodata.client')

        if odata_version == AsyncClient.ODATA_VERSION_2:

            # sanitize url
            url = url.rstrip('/') + '/'

            if metadata is None:
                metadata = await _fetch_metadata(connection, url, logger)
            else:
                logger.info('Using static metadata')

            if config is not None and namespaces is not None:
                raise PyODataException('You cannot pass namespaces and config at the same time')

            if config is None:
                config = pyodata.v2.model.Config()

            if namespaces is not None:
                warnings.warn("Passing namespaces directly is deprecated. Use class Config instead", DeprecationWarning)
                config.namespaces = namespaces

            # create model instance from received metadata
            logger.info('Creating OData Schema (version: %d)', odata_version)
            schema = pyodata.v2.model.MetadataBuilder(metadata, config=config).build()

            # create service instance based on model we have
            logger.info('Creating OData Service (version: %d)', odata_version)
            service = pyodata.v2.async_service.AsyncService(url, schema, connection, config=config)

            return service

        raise PyODataException(f'No implementation for selected odata version {odata_version}')
