import logging

from requests import HTTPError

from etl.common.extractors import rest_get


class ResourceNotFound(IOError):
    pass


COIN_GECKO_API_ROOT = "https://api.coingecko.com/api/v3/"


def get_resource_instance_info(base_url, url_template, ids_, skip_if_not_found=False):
    logging.info('Getting %i resources at %s :: %s', len(ids_), base_url, url_template)

    def _get(resource_url, id_):
        url = url_template.format(instance_id=id_)
        try:
            return rest_get(resource_url + url)
        except HTTPError as error:
            if error.response.status_code == 404:
                reason = error.response.text
                if skip_if_not_found:
                    logging.warning('Skipping resource %s at %s Reason:\n\t%s',
                                    id_, resource_url, reason)
                    return
                raise ResourceNotFound(reason)
    return {id_: _get(base_url, id_) for id_ in ids_}
