import http from 'k6/http';
import { check } from 'k6';

const baseURL = __ENV.BASE_URL || 'http://127.0.0.1:9082';
const tenantID = __ENV.TENANT_ID || 'perf-tenant';
const filePath = __ENV.FILE_PATH;
const fileName = __ENV.FILE_NAME || 'upload.bin';
const contentType = __ENV.CONTENT_TYPE || 'application/octet-stream';

if (!filePath) {
	throw new Error('FILE_PATH must point to a local file, for example FILE_PATH=/tmp/dms-perf/25mb.bin');
}

const fileBytes = open(filePath, 'b');

export const options = {
	vus: Number(__ENV.VUS || 10),
	duration: __ENV.DURATION || '1m',
	thresholds: {
		http_req_failed: ['rate<0.01'],
		http_req_duration: ['p(95)<5000'],
	},
};

export default function () {
	const payload = {
		bin: http.file(fileBytes, fileName, contentType),
	};

	const response = http.post(`${baseURL}/dss/api/put/${tenantID}`, payload);

	check(response, {
		'upload status is 200': (r) => r.status === 200,
		'upload response is success': (r) => {
			try {
				const body = JSON.parse(r.body);
				return body.error === false && typeof body.data?.cid === 'string';
			} catch (_) {
				return false;
			}
		},
	});
}
