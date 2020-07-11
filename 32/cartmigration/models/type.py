# from cartmigration.libs.base_model import BaseModel


class LeType():
	def source_cart(self):
		source_cart = {
			'nopcommerce': 'Nopcommerce',
			'volusion': 'Volusion',
			'oscommerce': 'OsCommerce',
			'virtuemart': 'VirtueMart',
			'zencart': 'ZenCart',
			'interspire': 'Interspire',
			'shopify': 'Shopify',
			'bigcommerce': 'BigCommerce',
			'mivamerchant': 'Miva Merchant',
			'woocommerce': 'Woocommerce',
			'prestashop': "Prestashop",
			'opencart': 'Opencart',
			'3dcart': '3dCart',
			'bigcartel': 'Big Cartel',
			'amazonstore': 'Amazon Store',
			'yahoostore': 'Yahoo Store/Aabaco',
			'jigoshop': 'Jigoshop',
			'weebly': 'Weebly',
			'squarespace': 'Squarespace',
			'magento': 'Magento',
			'pinnaclecart': 'PinnacleCart',
			'wpecommerce': 'Wp-Ecommerce',
			'wpestore'   :'WpEstore',
			'cubecart': "CubeCart",
			'oxideshop': "OXID-eShop",
			'cscart': "CS-Cart",
			'hikashop': "Hikashop",
			'xcart': "X-Cart",
			'jshop': "Jshop Server",
			'wix': 'Wix',
			'americommerce': 'Americommerce',
			'ecwid': "Ecwid",
			'ekm': "Ekm",
			'cart66': "Cart66",
			'abantecart': "Abantecart",
			'ubercart': 'Ubercart',
			'loaded': 'Loadedcommerce',
			'custom': 'Cart Custom',
			'shopp': 'Shopp',
			'marketpress': 'Marketpress',
			'epage': 'Epage',
			'summercart': 'Summercart'
		}
		return source_cart

	def target_cart(self):
		target_cart = {
			'shopify': 'Shopify',
            'bigcommerce': 'BigCommerce',
            'opencart': 'Opencart',
            'magento': 'Magento',
            '3dcart': "3dCart",
			'woocommerce': "WooCommerce",
			'xcart': "X-Cart",
			'prestashop': "Prestashop",
			'wix': "Wix",
		}
		return target_cart
