from flask import Blueprint, request as flask_request, jsonify, send_file
from werkzeug.datastructures import ImmutableMultiDict
from cartmigration.libs.base_controller import BaseController
from cartmigration.libs.utils import *

file_path = Blueprint('file_path', __name__)

@file_path.route('/upload', methods = ['POST', 'OPTIONS'])
def upload():
	form_data = ImmutableMultiDict(flask_request.form)
	form_data = form_data.to_dict(True)
	for key_form, data in form_data.items():
		data_decode = json_decode(data)
		if data_decode:
			form_data[key_form] = data_decode
	files_data = flask_request.files
	migration_id = form_data['migration_id']
	controller = BaseController()
	router = get_model('basecart')
	getattr(router, 'set_migration_id')(migration_id)
	controller.set_migration_id(migration_id)
	controller.init_cart()
	notice = controller.get_notice()
	cart_url = notice['src']['cart_url']
	folder_upload = getattr(router, 'create_folder_upload')(cart_url)
	upload_res = dict()
	for key, file in files_data.items():
		key = key.replace('file_', '')
		try:
			path_upload = get_pub_path() + '/' + DIR_UPLOAD + '/' + to_str(migration_id) + '/' + folder_upload
			if not (os.path.isdir(path_upload)):
				os.makedirs(path_upload)
			file.save(path_upload + '/' + file.filename)
			upload_res[key] = {
				'result': 'success',
				'elm': '#result-upload-' + str(key),
				'msg': "<div class='uir-success'> Uploaded successfully.</div>",
				'file': file.filename
			}
		except:
			error = traceback.format_exc()
			log(error, migration_id)
			upload_res[key] = {
				'result': 'error',
				'elm': '#result-upload-' + str(key),
				'msg': "<div class='uir-warning'> Uploaded error.</div>",

			}
	notice['src']['config']['folder'] = folder_upload
	# notice['upload_res'] = upload_res
	form_data['upload_res'] = upload_res
	controller.set_notice(notice)
	controller.save_notice()
	# notice['migration_id'] = migration_id
	buffer = dict()
	buffer['controller'] = 'migration'
	buffer['action'] = 'display_upload'
	buffer['data'] = form_data
	# clone_code_for_migration_id(notice['migration_id'])
	display_upload = start_subprocess(migration_id, buffer, True)
	return jsonify(response_success(display_upload))

@file_path.route('/download', methods = ['get'])
def download():
	migration_id = flask_request.args.get('migration_id')
	file_name = flask_request.args.get('file_name')
	if not migration_id:
		return ''
	if not file_name:
		return ''
	file_csv = get_pub_path() + '/media/' + to_str(migration_id) + '/' + file_name
	if not os.path.isfile(file_csv):
		return ''
	return send_file(file_csv,
	                 mimetype = 'text/csv',
	                 attachment_filename = os.path.basename(file_csv),
	                 as_attachment = True)

@file_path.route('/shopify/reviews/download', methods = ['get'])  # this is a job for GET, not POST
def download_review_shopify():
	migration_id = flask_request.args.get('migration_id')
	file_name = flask_request.args.get('file_name')
	if not migration_id:
		return ''
	if not file_name:
		file_name = 'reviews.csv'
	file_csv = get_pub_path() + '/media/' + to_str(migration_id) + '/' + file_name
	if not os.path.isfile(file_csv):
		return ''
	return send_file(file_csv,
	                 mimetype = 'text/csv',
	                 attachment_filename = 'reviews.csv',
	                 as_attachment = True)

@file_path.route('/3dcart/image', methods = ['get'])
def image_3dcart():
	migration_id = flask_request.args.get('migration_id')
	file_name = flask_request.args.get('file_name')
	if not migration_id:
		return ''
	if not file_name:
		return ''
	file_csv = get_pub_path() + '/media/' + to_str(migration_id) + '/3dcart/zip/' + file_name
	if not os.path.isfile(file_csv):
		return ''
	return send_file(file_csv,
	                 mimetype = 'zip',
	                 attachment_filename = os.path.basename(file_csv),
	                 as_attachment = True)
