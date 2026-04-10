#!/bin/bash
# Rebuild heat_data.json from KB entries and copy to docs/ for GitHub Pages.
# Run from repo root: ./build.sh

set -e

echo "Generating heat scores..."
cd kb/scripts
python3 -c "
import json, os, sys
sys.path.insert(0, '.')
from county_heat_score import load_fips_lookup, score_counties, FIPS_TO_COUNTY

load_fips_lookup()

county_data = score_counties('../../kb/../kb/../kb', '../../kb')

# Try standard paths
igsa_candidates = [
    os.path.expanduser('~/tcp-kb-internal/igsa-holders'),
    '../igsa-holders',
    '../../igsa-holders',
]
igsa_path = None
for p in igsa_candidates:
    if os.path.isdir(p):
        igsa_path = p
        break

if not igsa_path:
    print('Warning: igsa-holders KB not found, scoring without IGSA data', file=sys.stderr)
    county_data = score_counties(None, os.path.abspath('../../kb'))
else:
    county_data = score_counties(igsa_path, os.path.abspath('../../kb'))

output = []
for fips, data in sorted(county_data.items(), key=lambda x: -x[1]['score']):
    if data['score'] <= 0:
        continue
    county_name = FIPS_TO_COUNTY.get(fips, fips)
    signals_detail = {}
    for stype, titles in data['signals'].items():
        if titles:
            signals_detail[stype] = {
                'count': len(titles),
                'entries': titles[:5]
            }
    output.append({
        'fips': fips,
        'county': county_name,
        'state': data['state'],
        'score': data['score'],
        'signal_types': len([t for t in data['signals'] if data['signals'][t]]),
        'signals': signals_detail,
    })

with open('../../docs/heat_data.json', 'w') as f:
    json.dump(output, f)

print(f'Wrote {len(output)} counties to docs/heat_data.json')
print(f'Max score: {output[0][\"score\"]} ({output[0][\"county\"]})')
"

cd ../..
echo "Done. Commit and push to update GitHub Pages."
